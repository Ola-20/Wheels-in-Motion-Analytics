variable "project_id" {
  description = "The ID of the GCP project where resources will be deployed"
  type        = string
  default     = "bicycle-renting-proc-analytics"
}

variable "region" {
  description = "Default region"
  type        = string
  default     = "australia-southeast1"
}

variable "zone" {
  description = "Default zone"
  type        = string
  default     = "australia-southeast1-a"
}

variable "sa_account_id" {
  description = "Service account ID (prefix of the SA email)"
  type        = string
  default     = "de-service-account"
}

variable "machine_type" {
  description = "Compute Engine machine type"
  type        = string
  default     = "e2-standard-4"
}

variable "image" {
  description = "Boot image family or project/image"
  type        = string
  default     = "debian-cloud/debian-12"
}

variable "boot_disk_size" {
  description = "Boot disk size in GB"
  type        = number
  default     = 30
}

variable "gcs_bucket_name" {
  description = "Globally unique GCS bucket name"
  type        = string
  default = "bicycle-renting-proc-analytics-bucket-12345"  # Change this to a unique name
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "bicycle_rent_warehouse"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "australia-southeast1"
}
