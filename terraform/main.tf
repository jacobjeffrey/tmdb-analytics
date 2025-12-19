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
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "bq_writer" {
  dataset_id = google_bigquery_dataset.tmdb.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}