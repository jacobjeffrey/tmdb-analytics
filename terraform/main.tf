terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "raw" {
  # Good practice: Include region in the resource if it differs from provider default, 
  # but here we can rely on the provider or set it explicitly:
  name     = "tmdb-raw-${random_id.bucket_prefix.hex}"
  location = var.region
}

resource "google_bigquery_dataset" "tmdb" {
  dataset_id = "tmdb_analytics"
  location   = var.region
}

resource "google_service_account" "pipeline" {
  account_id   = "tmdb-pipeline"
  display_name = "TMDB pipeline runner"
}

resource "google_storage_bucket_iam_member" "raw_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "bq_writer" {
  dataset_id = google_bigquery_dataset.tmdb.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "dev_writer" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "user:${var.dev_user_email}"
}

resource "google_storage_bucket_iam_member" "dev_reader" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "user:${var.dev_user_email}"
}

# (This allows you to assign this SA to the job later)
resource "google_service_account_iam_member" "pipeline_user" {
  service_account_id = google_service_account.pipeline.id
  role               = "roles/iam.serviceAccountUser"
  member             = "user:${var.dev_user_email}" # Or the identity deploying the job
}

# Outputs for easy reference
output "gcs_bucket_name" {
  description = "Name of the GCS bucket for raw data"
  value       = google_storage_bucket.raw.name
}

output "service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline.email
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.tmdb.dataset_id
}