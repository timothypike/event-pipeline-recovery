# One role per function, each limited to the buckets, prefixes, and actions that function needs.

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

locals {
  raw        = aws_s3_bucket.zone["raw"].arn
  curated    = aws_s3_bucket.zone["curated"].arn
  quarantine = aws_s3_bucket.zone["quarantine"].arn

  logs = {
    Effect   = "Allow"
    Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.me.account_id}:log-group:/aws/lambda/${local.prefix}-*:*"
  }
  kms_rw = { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.data.arn }
  kms_r  = { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.data.arn }

  policies = {
    ingest = [
      local.logs, local.kms_rw,
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${local.raw}/raw/*", "${local.raw}/replay/*"] },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = ["${local.curated}/events/*", "${local.curated}/manifests/*"] },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = ["${local.quarantine}/rejects/*", "${local.quarantine}/batches/*"] },
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], Resource = aws_sqs_queue.ingest.arn },
    ]
    replay = [
      local.logs, local.kms_rw,
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage"], Resource = aws_sqs_queue.dlq.arn },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${local.raw}/raw/*", "${local.raw}/replay/*"] },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = "${local.raw}/replay/*" },
    ]
    list_raw = [
      local.logs,
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.raw, Condition = { StringLike = { "s3:prefix" = ["raw/*"] } } },
    ]
    check = [
      local.logs, local.kms_r,
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.raw, Condition = { StringLike = { "s3:prefix" = ["raw/*"] } } },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.curated, Condition = { StringLike = { "s3:prefix" = ["manifests/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.curated}/manifests/*" },
    ]
    stream_to_raw = [
      local.logs, local.kms_rw,
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = "${local.raw}/raw/*" },
      { Effect = "Allow", Action = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:ListStreams"], Resource = local.stream_arn },
      { Effect = "Allow", Action = ["sqs:SendMessage"], Resource = local.stream_dlq_arn },
    ]
  }
  stream_arn     = var.enable_kinesis ? aws_kinesis_stream.events[0].arn : "arn:aws:kinesis:${var.region}:${data.aws_caller_identity.me.account_id}:stream/disabled"
  stream_dlq_arn = var.enable_kinesis ? aws_sqs_queue.stream_dlq[0].arn : "arn:aws:sqs:${var.region}:${data.aws_caller_identity.me.account_id}:disabled"
}

resource "aws_iam_role" "fn" {
  for_each           = toset(local.lambdas)
  name               = "${local.prefix}-${replace(each.key, "_", "-")}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy" "fn" {
  for_each = toset(local.lambdas)
  name     = "least-privilege"
  role     = aws_iam_role.fn[each.key].id
  policy   = jsonencode({ Version = "2012-10-17", Statement = local.policies[each.key] })
}

data "aws_iam_policy_document" "sfn_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.prefix}-backfill"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}

resource "aws_iam_role_policy" "sfn" {
  name = "invoke-pipeline"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = [for k in ["ingest", "list_raw", "check"] : aws_lambda_function.fn[k].arn]
    }]
  })
}

# ------------------------------------------------------------------ Glue compaction job
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-compact"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role_policy" "glue" {
  name = "least-privilege"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.zone["artifacts"].arn}/glue/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.curated,
      Condition = { StringLike = { "s3:prefix" = ["events/*", "compacted", "compacted/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${local.curated}/events/*", "${local.curated}/compacted/*"] },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:DeleteObject"],
      Resource = ["${local.curated}/compacted/*", "${local.curated}/compacted_*", "${local.curated}/compaction-runs/*"] },
      local.kms_rw,
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.me.account_id}:log-group:/aws-glue/*" },
      { Effect = "Allow", Action = ["cloudwatch:PutMetricData"], Resource = "*",
      Condition = { StringEquals = { "cloudwatch:namespace" = "Glue" } } },
    ]
  })
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "run-compaction"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      Resource = aws_glue_job.compact.arn },
      { Effect = "Allow", Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
      Resource = "arn:aws:events:${var.region}:${data.aws_caller_identity.me.account_id}:rule/StepFunctionsGetEventsForGlueJobsRule" },
    ]
  })
}
