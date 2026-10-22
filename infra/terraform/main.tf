data "google_project" "this" {
  project_id = var.project_id
}

locals {
  labels = {
    app     = "cvm-bigquery-lakehouse"
    managed = "terraform"
  }

  datasets = {
    raw     = "Landing: dado cru da CVM (tudo STRING). Contém CPF e nome de diretor do cadastro: acesso restrito."
    staging = "Silver: tipado, deduplicado, sem CPF (LGPD: minimização)."
    marts   = "Gold: dimensões, fatos e agregados para consumo."
  }
}

# --- APIs ---------------------------------------------------------------------

resource "google_project_service" "apis" {
  for_each = toset(concat(
    ["bigquery.googleapis.com", "storage.googleapis.com", "iam.googleapis.com"],
    var.create_composer ? ["composer.googleapis.com"] : [],
    var.billing_account_id != "" ? ["billingbudgets.googleapis.com"] : []
  ))

  service            = each.value
  disable_on_destroy = false
}

# --- Landing zone (GCS) -------------------------------------------------------

resource "google_storage_bucket" "landing" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy
  labels                      = local.labels

  lifecycle_rule {
    condition {
      age = var.landing_retention_days
    }

    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# --- BigQuery: um dataset por camada ----------------------------------------------

resource "google_bigquery_dataset" "layers" {
  for_each = local.datasets

  dataset_id                 = each.key
  location                   = var.bq_location
  description                = each.value
  delete_contents_on_destroy = var.force_destroy
  labels                     = local.labels

  depends_on = [google_project_service.apis]
}

# --- Contas de serviço (menor privilégio) ----------------------------------------

resource "google_service_account" "pipeline" {
  account_id   = "cvm-pipeline"
  display_name = "CVM pipeline (ingestão + dbt; usada pelo Composer)"
}

resource "google_service_account" "ci" {
  account_id   = "cvm-ci"
  display_name = "CVM CI (dbt em datasets efêmeros)"
}

resource "google_service_account" "reporter" {
  account_id   = "cvm-reporter"
  display_name = "CVM consumo (lê só o marts)"
}

# pipeline: escreve no landing e em todas as camadas
resource "google_storage_bucket_iam_member" "pipeline_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  for_each = google_bigquery_dataset.layers

  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# ci: lê o raw; cria e apaga datasets efêmeros dbt_ci_* (exige dataEditor no projeto: use um
# projeto dedicado/sandbox para CI)
resource "google_bigquery_dataset_iam_member" "ci_raw_viewer" {
  dataset_id = google_bigquery_dataset.layers["raw"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_project_iam_member" "ci_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.ci.email}"
}

# reporter: só o marts
resource "google_bigquery_dataset_iam_member" "reporter_marts" {
  dataset_id = google_bigquery_dataset.layers["marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.reporter.email}"
}

# quem executa consultas precisa de jobUser no projeto
resource "google_project_iam_member" "job_user" {
  for_each = {
    pipeline = google_service_account.pipeline.email
    ci       = google_service_account.ci.email
    reporter = google_service_account.reporter.email
  }

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${each.value}"
}

# --- Cloud Composer (opcional, caro) -------------------------------------------

resource "google_project_iam_member" "composer_worker" {
  count = var.create_composer ? 1 : 0

  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_composer_environment" "main" {
  count = var.create_composer ? 1 : 0

  name   = "cvm-composer"
  region = var.region
  labels = local.labels

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = var.composer_image_version

      pypi_packages = {
        "dbt-bigquery"          = ">=1.9"
        "google-cloud-bigquery" = ">=3.25"
        "google-cloud-storage"  = ">=2.18"
        "requests"              = ">=2.32"
        "pandas"                = ">=2.2"
      }

      env_variables = {
        GCP_PROJECT     = var.project_id
        CVM_BUCKET      = google_storage_bucket.landing.name
        CVM_PREFIX      = "cvm"
        CVM_MONTHS_BACK = "2"
        DBT_TARGET      = "dev" # oauth com as credenciais da conta de serviço do ambiente
      }
    }

    node_config {
      service_account = google_service_account.pipeline.email
    }
  }

  depends_on = [google_project_iam_member.composer_worker]
}

# --- Orçamento com alertas (opcional) --------------------------------------------

resource "google_billing_budget" "monthly" {
  count = var.billing_account_id != "" ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "cvm-bigquery-lakehouse"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "BRL"
      units         = tostring(var.monthly_budget_brl)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }

  threshold_rules {
    threshold_percent = 0.9
  }

  threshold_rules {
    threshold_percent = 1.0
  }

  depends_on = [google_project_service.apis]
}
