output "bucket_name" {
  description = "Private lake bucket for raw, curated, quarantine, and Athena results."
  value       = aws_s3_bucket.lake.bucket
}

output "glue_job_name" {
  description = "AWS Glue ETL job name."
  value       = aws_glue_job.transform.name
}

output "athena_database" {
  description = "Glue Data Catalog database queried by Athena."
  value       = aws_glue_catalog_database.analytics.name
}

output "athena_table" {
  description = "Curated events table name."
  value       = aws_glue_catalog_table.curated.name
}

output "athena_workgroup" {
  description = "Athena workgroup with an enforced encrypted S3 results path."
  value       = aws_athena_workgroup.analytics.name
}

output "athena_output_uri" {
  description = "S3 location for encrypted Athena query results; expires after seven days."
  value       = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
}
