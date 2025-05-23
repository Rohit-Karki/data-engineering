terraform {
    required_version = ">= 1.0"
    backend "local" {}
  required_providers {
    google = {
        source = "hashicorp/google"
        version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = files("rohit.json")

  project = var.project
  region = var.region
  zone = "us-central1-c"
}
# Resource names are the internal names that we use in our Terraform configurations to 
# refer to each resource and have no impact on the actual infrastructure
# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}


# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}