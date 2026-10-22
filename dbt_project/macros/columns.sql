{#- Tolerância a mudança de schema no raw. O raw tem as colunas de TODOS os arquivos já
    carregados; uma coluna pode não existir (ainda) ou ter mudado de nome entre meses
    (CNPJ_FUNDO -> CNPJ_FUNDO_CLASSE). `col_or_null` devolve a coluna se existir, senão NULL;
    `first_of` faz o coalesce entre nomes alternativos. -#}
{% macro col_or_null(relation, column) -%}
    {%- if execute -%}
        {%- set existing = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
        {%- if column | lower in existing -%}{{ adapter.quote(column) }}{%- else -%}cast(null as {{ dbt.type_string() }}){%- endif -%}
    {%- else -%}
        cast(null as {{ dbt.type_string() }})
    {%- endif -%}
{%- endmacro %}

{% macro first_of(relation, names) -%}
    coalesce({%- for name in names -%}{{ col_or_null(relation, name) }}{{ ", " if not loop.last }}{%- endfor -%})
{%- endmacro %}

{#- CNPJ/CPF formatados -> só dígitos (sem regex, que muda de sintaxe entre bancos). -#}
{% macro digits_only(expr) -%}
    replace(replace(replace(replace(trim({{ expr }}), '.', ''), '/', ''), '-', ''), ' ', '')
{%- endmacro %}

{% macro sha256_hex(expr) -%} {{ return(adapter.dispatch('sha256_hex', 'cvm_fundos')(expr)) }} {%- endmacro %}
{% macro default__sha256_hex(expr) -%} sha256(cast({{ expr }} as {{ dbt.type_string() }})) {%- endmacro %}
{% macro bigquery__sha256_hex(expr) -%} to_hex(sha256(cast({{ expr }} as {{ dbt.type_string() }}))) {%- endmacro %}
