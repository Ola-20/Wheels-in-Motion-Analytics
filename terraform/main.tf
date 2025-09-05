#gcloud auth application-default login ---(run this on your terminal and select a google acct so that Terraform runs first time)---
#gcloud config set project bicycle-renting-proc-analytics ---- (run this next, i want trerraform to deploy into this project
                                                                 #called : bicycle-renting-proc-analytics)----






terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3.0"
}

# ---- Provider (first run uses your ADC: gcloud auth application-default login) ----
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
  # AFTER FIRST APPLY, add this line to run as the SA (no keys):
  # impersonate_service_account = google_service_account.bicycle_rent_sa.email
}

# Who is the current logged-in user? (avoid hardcoding your email), this will be grabbed from your gcloud auth application-default login
data "google_client_openid_userinfo" "me" {}

# ---- Enable Required APIs ----
resource "google_project_service" "compute" {
  project            = var.project_id
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

# ---- Service Account ----
resource "google_service_account" "bicycle_rent_sa" {
  account_id   = var.sa_account_id                 # e.g., "de-service-account"
  display_name = "Service Account for DE workloads"
  depends_on   = [
    google_project_service.iam,
    google_project_service.compute,
    google_project_service.storage,
    google_project_service.bigquery
  ]
}

# ---- Allow YOUR USER to impersonate the SA (no key files needed) ----
resource "google_service_account_iam_member" "allow_impersonation" {
  service_account_id = google_service_account.bicycle_rent_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${data.google_client_openid_userinfo.me.email}"
}

# ---- IAM Roles for SA (explicit, no loop) ----
resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.bicycle_rent_sa.email}"
  depends_on = [google_service_account.bicycle_rent_sa]
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.bicycle_rent_sa.email}"
  depends_on = [google_service_account.bicycle_rent_sa]
}

resource "google_project_iam_member" "compute_admin" {
  project = var.project_id
  role    = "roles/compute.admin"
  member  = "serviceAccount:${google_service_account.bicycle_rent_sa.email}"
  depends_on = [google_service_account.bicycle_rent_sa]
}

# ---- Compute Engine VM (optional) ----
resource "google_compute_instance" "bicycle_rent_vm" {
  name         = "bicycle-rent-vm"
  machine_type = var.machine_type                 # e.g., "e2-micro" or "e2-small"
  tags         = [var.project_id]

  # This will allow me to change machine_type without recreating the VM
  allow_stopping_for_update = true

  # Attach the newly created SA so the VM has the permissions above
  service_account {
    email  = google_service_account.bicycle_rent_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  boot_disk {
    initialize_params {
      image = var.image                           # e.g., "debian-cloud/debian-12"
      size  = var.boot_disk_size                  # e.g., 10
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"
    access_config {}                              # ephemeral external IP
  }

  labels = { environment = "dev" }

  depends_on = [
    google_project_service.compute,
    google_project_iam_member.bq_admin,
    google_project_iam_member.storage_admin,
    google_project_iam_member.compute_admin
  ]
}

# ---- Cloud Storage bucket ----
resource "google_storage_bucket" "de_bucket" {
  name                        = var.gcs_bucket_name   # must be globally unique
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false

  labels = {
    name        = "gcsfordeprojects"
    environment = "dev"
  }

  depends_on = [google_project_service.storage]
}

# ---- BigQuery dataset ----
resource "google_bigquery_dataset" "de_bq" {
  dataset_id                 = var.bq_dataset_id      # e.g., "de_warehouse"
  location                   = var.bq_location        # e.g., "australia-southeast1" or "US"
  delete_contents_on_destroy = true

  labels = { environment = "dev" }

  depends_on = [google_project_service.bigquery]
}

# ---- (Optional) Outputs ----
output "service_account_email" {
  value = google_service_account.bicycle_rent_sa.email
}

output "vm_name" {
  value = google_compute_instance.bicycle_rent_vm.name
}
