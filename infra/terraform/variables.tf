variable "project_id" {
  description = "ID do projeto GCP. Use um projeto DEDICADO ao portfólio: facilita medir custo e destruir tudo."
  type        = string
}

variable "region" {
  description = "Região do bucket e do Composer. southamerica-east1 = São Paulo."
  type        = string
  default     = "southamerica-east1"
}

variable "bq_location" {
  description = "Localização dos datasets do BigQuery (precisa ser igual à do bucket para o load job)."
  type        = string
  default     = "southamerica-east1"
}

variable "bucket_name" {
  description = "Nome globalmente único do bucket de landing."
  type        = string
}

variable "landing_retention_days" {
  description = "Dias até os arquivos do landing zone serem apagados (o raw no BigQuery é a fonte de verdade)."
  type        = number
  default     = 90
}

variable "force_destroy" {
  description = "Permite destruir bucket e datasets mesmo com conteúdo. Bom para demonstração; remova em produção."
  type        = bool
  default     = true
}

# --- Custo ------------------------------------------------------------------

variable "create_composer" {
  description = <<-EOT
    Cria um ambiente Cloud Composer (Airflow gerenciado). DESLIGADO por padrão: o ambiente fica
    ligado 24h e custa centenas de reais por mês. Ligue só para demonstrar e rode `terraform
    destroy` logo depois. Sem ele, a DAG roda em qualquer Airflow (ex.: docker local).
  EOT
  type        = bool
  default     = false
}

variable "composer_image_version" {
  description = "Imagem do Composer (confira as versões vigentes na documentação do GCP)."
  type        = string
  default     = "composer-3-airflow-2.10.5"
}

variable "billing_account_id" {
  description = "ID da conta de faturamento. Se informado, cria um orçamento com alertas."
  type        = string
  default     = ""
}

variable "monthly_budget_brl" {
  description = "Orçamento mensal em reais para os alertas (50%, 90% e 100%)."
  type        = number
  default     = 100
}
