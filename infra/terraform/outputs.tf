output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "datasets" {
  value = { for name, dataset in google_bigquery_dataset.layers : name => dataset.dataset_id }
}

output "service_accounts" {
  value = {
    pipeline = google_service_account.pipeline.email
    ci       = google_service_account.ci.email
    reporter = google_service_account.reporter.email
  }
}

output "composer_dag_prefix" {
  description = "Pasta de DAGs do Composer (vazio se create_composer = false)."
  value       = var.create_composer ? google_composer_environment.main[0].config[0].dag_gcs_prefix : ""
}
