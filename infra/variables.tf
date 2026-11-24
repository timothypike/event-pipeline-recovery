variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource."
  type        = string
  default     = "epr"
}

variable "pandas_layer_arn" {
  description = "AWS SDK for pandas layer (provides pyarrow for Parquet). Must match the region."
  type        = string
  default     = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:32"
}

variable "max_receive_count" {
  description = "Attempts before a failed batch moves to the dead-letter queue."
  type        = number
  default     = 3
}

variable "log_retention_days" {
  type    = number
  default = 7
}

variable "enable_kinesis" {
  description = "Add the optional Kinesis streaming input (about $0.015/hour for one provisioned shard)."
  type        = bool
  default     = false
}
