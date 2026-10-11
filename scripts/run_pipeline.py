"""Run the provisioned Glue job and register its new Athena partitions."""

from __future__ import annotations

import argparse
import time

import boto3


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", default="events_curated")
    args = parser.parse_args()

    glue = boto3.client("glue", region_name=args.region)
    athena = boto3.client("athena", region_name=args.region)
    run_id = glue.start_job_run(JobName=args.job_name)["JobRunId"]
    print(f"Started Glue job {args.job_name}: {run_id}")

    while True:
        run = glue.get_job_run(JobName=args.job_name, RunId=run_id)["JobRun"]
        state = run["JobRunState"]
        print(f"Glue state: {state}")
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "TIMEOUT", "ERROR", "STOPPED", "EXPIRED"}:
            raise RuntimeError(run.get("ErrorMessage", f"Glue job ended in {state}"))
        time.sleep(15)

    query = f"MSCK REPAIR TABLE {args.database}.{args.table}"
    query_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": args.database},
    )["QueryExecutionId"]
    print(f"Registering new partitions in Athena: {query_id}")

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = result["Status"]["State"]
        print(f"Athena state: {state}")
        if state == "SUCCEEDED":
            print("Pipeline complete. Open the Streamlit dashboard to explore the data.")
            return
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(result["Status"].get("StateChangeReason", f"Athena query {state}"))
        time.sleep(2)


if __name__ == "__main__":
    main()
