output "raw_bucket" { value = aws_s3_bucket.zone["raw"].id }
output "curated_bucket" { value = aws_s3_bucket.zone["curated"].id }
output "quarantine_bucket" { value = aws_s3_bucket.zone["quarantine"].id }
output "dlq_url" { value = aws_sqs_queue.dlq.url }
output "functions" { value = { for k, f in aws_lambda_function.fn : k => f.function_name } }
output "backfill_state_machine" { value = aws_sfn_state_machine.backfill.arn }
output "glue_database" { value = aws_glue_catalog_database.db.name }
output "athena_workgroup" { value = aws_athena_workgroup.wg.name }
output "glue_job" { value = aws_glue_job.compact.name }
output "kinesis_stream" { value = var.enable_kinesis ? aws_kinesis_stream.events[0].name : null }
