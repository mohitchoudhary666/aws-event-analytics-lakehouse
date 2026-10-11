"""Validate raw clickstream CSV and write partitioned Parquet plus quarantine."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "QUARANTINE_PATH", "MAX_REJECT_RATE"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw = (
    spark.read.option("header", True)
    .option("mode", "PERMISSIVE")
    .csv(args["SOURCE_PATH"])
)
has_event_id = F.col("event_id").isNotNull() & (F.trim(F.col("event_id")) != "")
identified = raw.filter(has_event_id).dropDuplicates(["event_id"])
unidentified = raw.filter(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""))
raw = identified.unionByName(unidentified)

typed = (
    raw.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double"))
    .withColumn("region", F.lower(F.trim(F.col("region"))))
    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn("event_date", F.to_date("event_timestamp"))
)

allowed_events = ["product_view", "add_to_cart", "checkout", "purchase"]
required_columns = ["event_id", "user_id", "session_id", "product_id", "category", "region"]
missing_required = None
for name in required_columns:
    condition = F.col(name).isNull() | (F.trim(F.col(name)) == "")
    missing_required = condition if missing_required is None else missing_required | condition

invalid = (
    missing_required
    | F.col("event_timestamp").isNull()
    | (~F.col("event_type").isin(allowed_events))
    | F.col("quantity").isNull()
    | F.col("unit_price_usd").isNull()
    | (F.col("quantity") < 0)
    | (F.col("unit_price_usd") < 0)
)

tagged = typed.withColumn(
    "quality_issue",
    F.when(missing_required, F.lit("missing_required_field"))
    .when(F.col("event_timestamp").isNull(), F.lit("invalid_timestamp"))
    .when(~F.col("event_type").isin(allowed_events), F.lit("unsupported_event_type"))
    .when(F.col("quantity").isNull() | (F.col("quantity") < 0), F.lit("invalid_quantity"))
    .when(F.col("unit_price_usd").isNull() | (F.col("unit_price_usd") < 0), F.lit("invalid_price")),
)

total_count = tagged.count()
valid = tagged.filter(~invalid).drop("quality_issue")
rejected = tagged.filter(invalid)
valid_count = valid.count()
rejected_count = total_count - valid_count
reject_rate = rejected_count / total_count if total_count else 1.0

print(
    f"DATA_QUALITY total={total_count} accepted={valid_count} rejected={rejected_count} "
    f"reject_rate={reject_rate:.4f}"
)

if rejected_count:
    (
        rejected.withColumn("ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .partitionBy("event_date", "region")
        .json(args["QUARANTINE_PATH"])
    )

if not valid_count:
    raise ValueError("No valid events remain after data-quality checks.")
if reject_rate > float(args["MAX_REJECT_RATE"]):
    raise ValueError(
        f"Rejected {reject_rate:.1%} of input rows, exceeding configured limit "
        f"{float(args['MAX_REJECT_RATE']):.1%}. Inspect the quarantine prefix."
    )

# Full refresh from the retained raw prefix makes reruns deterministic:
# duplicate event IDs across separate uploads are removed before this write.
(
    valid.write.mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("event_date", "region")
    .parquet(args["TARGET_PATH"])
)

job.commit()

