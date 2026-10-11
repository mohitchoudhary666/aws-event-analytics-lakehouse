resource "aws_s3_bucket" "lake" {
  bucket_prefix = "${var.project_name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-athena-query-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
    noncurrent_version_expiration { noncurrent_days = 7 }
  }
}

resource "aws_s3_object" "glue_script" {
  bucket       = aws_s3_bucket.lake.id
  key          = "scripts/transform_events.py"
  source       = "${path.module}/../glue/jobs/transform_events.py"
  source_hash  = filemd5("${path.module}/../glue/jobs/transform_events.py")
  content_type = "text/x-python"
}

resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue-job"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "${var.project_name}-scoped-data-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.lake.arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["raw/*", "curated/*", "quarantine/*", "scripts/*", "temporary/*"]
          }
        }
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/raw/*",
          "${aws_s3_bucket.lake.arn}/scripts/*",
          "${aws_s3_bucket.lake.arn}/temporary/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/curated/*",
          "${aws_s3_bucket.lake.arn}/quarantine/*",
          "${aws_s3_bucket.lake.arn}/temporary/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup"]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*:*"
      }
    ]
  })
}

resource "aws_glue_catalog_database" "analytics" {
  name        = "event_analytics"
  description = "Catalog for the synthetic clickstream analytics lake."
}

resource "aws_glue_catalog_table" "curated" {
  name          = "events_curated"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Validated synthetic clickstream events stored as partitioned Parquet."

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
    compressionType = "snappy"
    typeOfData     = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/curated/events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "events-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "event_timestamp"
      type = "timestamp"
    }
    columns {
      name = "user_id"
      type = "string"
    }
    columns {
      name = "session_id"
      type = "string"
    }
    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "device"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "int"
    }
    columns {
      name = "unit_price_usd"
      type = "double"
    }
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }
  partition_keys {
    name = "region"
    type = "string"
  }
}

resource "aws_athena_workgroup" "analytics" {
  name = "${var.project_name}-workgroup"
  configuration {
    enforce_workgroup_configuration = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

resource "aws_iam_role" "scheduler" {
  count = var.enable_daily_schedule ? 1 : 0
  name  = "${var.project_name}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  count = var.enable_daily_schedule ? 1 : 0
  name  = "${var.project_name}-start-glue-job"
  role  = aws_iam_role.scheduler[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun"]
      Resource = [aws_glue_job.transform.arn]
    }]
  })
}

resource "aws_glue_job" "transform" {
  name              = "${var.project_name}-transform"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language"                         = "python"
    "--SOURCE_PATH"                          = "s3://${aws_s3_bucket.lake.bucket}/raw/events/"
    "--TARGET_PATH"                          = "s3://${aws_s3_bucket.lake.bucket}/curated/events/"
    "--QUARANTINE_PATH"                      = "s3://${aws_s3_bucket.lake.bucket}/quarantine/events/"
    "--MAX_REJECT_RATE"                      = tostring(var.max_reject_rate)
    "--TempDir"                              = "s3://${aws_s3_bucket.lake.bucket}/temporary/"
    "--enable-metrics"                       = "true"
    "--enable-continuous-cloudwatch-log"      = "true"
    "--enable-observability-metrics"          = "true"
  }

  depends_on = [aws_iam_role_policy.glue, aws_s3_bucket_public_access_block.lake]
}

resource "aws_scheduler_schedule" "daily" {
  count = var.enable_daily_schedule ? 1 : 0
  name  = "${var.project_name}-daily-etl"
  flexible_time_window { mode = "OFF" }
  schedule_expression          = "cron(0 8 * * ? *)"
  schedule_expression_timezone = "UTC"
  state                        = "ENABLED"

  target {
    arn      = "arn:${data.aws_partition.current.partition}:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.scheduler[0].arn
    input    = jsonencode({ JobName = aws_glue_job.transform.name })
  }
}
