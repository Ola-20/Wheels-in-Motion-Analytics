terraform {
  backend "gcs" {
    bucket = "bicycle-renting-proc-analytics-tf-state"
    prefix = "envs/prod"
  }
}