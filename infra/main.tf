locals {
  prefix   = "${var.name}-${random_id.suffix.hex}"
  zones    = ["raw", "curated", "quarantine", "athena", "artifacts"]
  lambdas  = concat(["ingest", "replay", "list_raw", "check"], var.enable_kinesis ? ["stream_to_raw"] : [])
  src_root = "${path.module}/../src"
}

data "aws_caller_identity" "me" {}

resource "random_id" "suffix" {
  byte_length = 3
}

# ------------------------------------------------------------------ encryption
resource "aws_kms_key" "data" {
  description             = "${local.prefix} data at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.prefix}-data"
  target_key_id = aws_kms_key.data.key_id
}

# ------------------------------------------------------------------ storage zones
resource "aws_s3_bucket" "zone" {
  for_each      = toset(local.zones)
  bucket        = "${local.prefix}-${each.key}"
  force_destroy = true # demo stack: `terraform destroy` removes data too
}

resource "aws_s3_bucket_public_access_block" "zone" {
  for_each                = aws_s3_bucket.zone
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.zone["raw"].id
  versioning_configuration { status = "Enabled" } # raw is the system of record; overwrites stay recoverable
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.zone]
}

# ------------------------------------------------------------------ queues
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-ingest-dlq"
  message_retention_seconds = 1209600 # 14 days to notice and replay
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "ingest" {
  name                       = "${local.prefix}-ingest"
  visibility_timeout_seconds = 180 # 6x the Lambda timeout
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_sqs_queue_policy" "ingest" {
  queue_url = aws_sqs_queue.ingest.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.ingest.arn
      Condition = {
        ArnEquals    = { "aws:SourceArn" = aws_s3_bucket.zone["raw"].arn }
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.me.account_id }
      }
    }]
  })
}

resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.zone["raw"].id
  queue {
    queue_arn     = aws_sqs_queue.ingest.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = ".jsonl"
  }
  queue {
    queue_arn     = aws_sqs_queue.ingest.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "replay/"
    filter_suffix = ".jsonl"
  }
  depends_on = [aws_sqs_queue_policy.ingest]
}

# ------------------------------------------------------------------ code
data "archive_file" "code" {
  type        = "zip"
  source_dir  = local.src_root
  output_path = "${path.module}/build/pipeline.zip"
  excludes    = ["**/__pycache__/**"]
}

resource "aws_cloudwatch_log_group" "fn" {
  for_each          = toset(local.lambdas)
  name              = "/aws/lambda/${local.prefix}-${replace(each.key, "_", "-")}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "fn" {
  for_each         = toset(local.lambdas)
  function_name    = "${local.prefix}-${replace(each.key, "_", "-")}"
  role             = aws_iam_role.fn[each.key].arn
  runtime          = "python3.12"
  handler          = "pipeline.handlers.${each.key}"
  filename         = data.archive_file.code.output_path
  source_code_hash = data.archive_file.code.output_base64sha256
  layers           = [var.pandas_layer_arn]
  timeout          = each.key == "replay" ? 120 : 30
  memory_size      = 512
  environment {
    variables = {
      RAW_BUCKET        = aws_s3_bucket.zone["raw"].id
      CURATED_BUCKET    = aws_s3_bucket.zone["curated"].id
      QUARANTINE_BUCKET = aws_s3_bucket.zone["quarantine"].id
      DLQ_URL           = aws_sqs_queue.dlq.url
    }
  }
  depends_on = [aws_cloudwatch_log_group.fn]
}

resource "aws_lambda_event_source_mapping" "ingest" {
  event_source_arn = aws_sqs_queue.ingest.arn
  function_name    = aws_lambda_function.fn["ingest"].arn
  batch_size       = 1
  depends_on       = [aws_iam_role_policy.fn] # the role must be able to read the queue before the trigger exists
}

# ------------------------------------------------------------------ backfill orchestration
resource "aws_sfn_state_machine" "backfill" {
  name     = "${local.prefix}-backfill"
  role_arn = aws_iam_role.sfn.arn
  definition = jsonencode({
    Comment = "Reprocess every raw file that arrived in a date range, check nothing was lost, then compact the days."
    StartAt = "ListRaw"
    States = {
      ListRaw = {
        Type           = "Task"
        Resource       = "arn:aws:states:::lambda:invoke"
        Parameters     = { FunctionName = aws_lambda_function.fn["list_raw"].arn, "Payload.$" = "$" }
        ResultSelector = { "chunks.$" = "$.Payload.chunks", "count.$" = "$.Payload.count" }
        ResultPath     = "$.list"
        Next           = "IngestChunks"
      }
      IngestChunks = {
        Type           = "Map"
        ItemsPath      = "$.list.chunks"
        MaxConcurrency = 4
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "Ingest"
          States = {
            Ingest = {
              Type       = "Task"
              Resource   = "arn:aws:states:::lambda:invoke"
              Parameters = { FunctionName = aws_lambda_function.fn["ingest"].arn, "Payload.$" = "$" }
              Retry      = [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 5, MaxAttempts = 2, BackoffRate = 2 }]
              End        = true
            }
          }
        }
        ResultPath = null
        Next       = "Check"
      }
      Check = {
        Type           = "Task"
        Resource       = "arn:aws:states:::lambda:invoke"
        Parameters     = { FunctionName = aws_lambda_function.fn["check"].arn }
        ResultSelector = { "report.$" = "$.Payload" }
        ResultPath     = "$.check"
        Next           = "NothingLost"
      }
      NothingLost = {
        Type    = "Choice"
        Choices = [{ Variable = "$.check.report.ok", BooleanEquals = true, Next = "Compact" }]
        Default = "LossDetected"
      }
      LossDetected = {
        Type  = "Fail"
        Error = "DestinationCheckFailed"
        Cause = "A raw file has no manifest or a manifest does not balance. Compaction skipped."
      }
      Compact = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.compact.name
          Arguments = {
            "--start_date.$" = "$.start_date"
            "--end_date.$"   = "$.end_date"
          }
        }
        ResultSelector = { "state.$" = "$.JobRunState", "seconds.$" = "$.ExecutionTime" }
        ResultPath     = "$.compaction"
        End            = true
      }
    }
  })
}

# ------------------------------------------------------------------ compaction (Glue / PySpark)
resource "aws_s3_object" "compact_script" {
  bucket = aws_s3_bucket.zone["artifacts"].id
  key    = "glue/compact.py"
  source = "${local.src_root}/glue/compact.py"
  etag   = filemd5("${local.src_root}/glue/compact.py")
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${local.prefix}-compact"
  retention_in_days = var.log_retention_days
}

resource "aws_glue_job" "compact" {
  name              = "${local.prefix}-compact"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15
  max_retries       = 0
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.zone["artifacts"].id}/${aws_s3_object.compact_script.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--CURATED_BUCKET"                   = aws_s3_bucket.zone["curated"].id
    "--start_date"                       = "2026-11-01"
    "--end_date"                         = "2026-11-02"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue.name
    "--enable-metrics"                   = "true"
  }
}

# ------------------------------------------------------------------ optional streaming input (Kinesis)
# Off by default: a provisioned 1-shard stream costs about $0.015/hour while it exists.
# Streamed records are written to raw/ first, so they get the same quality gate, recovery, and checks as files.
resource "aws_kinesis_stream" "events" {
  count            = var.enable_kinesis ? 1 : 0
  name             = "${local.prefix}-events"
  shard_count      = 1
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = aws_kms_key.data.arn
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_sqs_queue" "stream_dlq" {
  count                     = var.enable_kinesis ? 1 : 0
  name                      = "${local.prefix}-stream-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_lambda_event_source_mapping" "stream" {
  count                              = var.enable_kinesis ? 1 : 0
  event_source_arn                   = aws_kinesis_stream.events[0].arn
  function_name                      = aws_lambda_function.fn["stream_to_raw"].arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 1000
  maximum_batching_window_in_seconds = 60 # one raw file per shard per minute, not one per record
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq[0].arn
    }
  }
  depends_on = [aws_iam_role_policy.fn]
}

# ------------------------------------------------------------------ catalog and query
resource "aws_glue_catalog_database" "db" {
  name = replace("${local.prefix}_events", "-", "_")
}

resource "aws_glue_catalog_table" "events" {
  database_name = aws_glue_catalog_database.db.name
  name          = "events"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification                        = "parquet"
    "projection.enabled"                  = "true" # no crawler: partitions are computed, so late data is queryable at once
    "projection.event_date.type"          = "date"
    "projection.event_date.format"        = "yyyy-MM-dd"
    "projection.event_date.range"         = "2025-01-01,NOW"
    "projection.event_date.interval"      = "1"
    "projection.event_date.interval.unit" = "DAYS"
    "storage.location.template"           = "s3://${aws_s3_bucket.zone["curated"].id}/events/event_date=$${event_date}/"
  }
  partition_keys {
    name = "event_date"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.zone["curated"].id}/events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "site_id"
      type = "string"
    }
    columns {
      name = "event_time_utc"
      type = "string"
    }
    columns {
      name = "metric"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "events_compacted" {
  database_name = aws_glue_catalog_database.db.name
  name          = "events_compacted"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification                        = "parquet"
    "projection.enabled"                  = "true" # no crawler: partitions are computed, so late data is queryable at once
    "projection.event_date.type"          = "date"
    "projection.event_date.format"        = "yyyy-MM-dd"
    "projection.event_date.range"         = "2025-01-01,NOW"
    "projection.event_date.interval"      = "1"
    "projection.event_date.interval.unit" = "DAYS"
    "storage.location.template"           = "s3://${aws_s3_bucket.zone["curated"].id}/compacted/event_date=$${event_date}/"
  }
  partition_keys {
    name = "event_date"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.zone["curated"].id}/compacted/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "site_id"
      type = "string"
    }
    columns {
      name = "event_time_utc"
      type = "string"
    }
    columns {
      name = "metric"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
  }
}

resource "aws_athena_workgroup" "wg" {
  name          = "${local.prefix}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB guardrail on cost
    result_configuration {
      output_location = "s3://${aws_s3_bucket.zone["athena"].id}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.data.arn
      }
    }
  }
}

# ------------------------------------------------------------------ alerting
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.prefix}-dlq-not-empty"
  alarm_description   = "A batch failed every retry and is waiting in the dead-letter queue. Run the replay function."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}
