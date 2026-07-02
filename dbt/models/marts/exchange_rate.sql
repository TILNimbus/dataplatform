{{
  config(
    materialized         = 'incremental',
    unique_key           = 'data_esatta',
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns',
    tags                 = ['marts', 'exchange_rate']
  )
}}

/*
  exchange_rate.sql
  ─────────────────────────────────────────────────────────────────
  Tabella finale ANALYTICS.EXCHANGE_RATE.
  Strategia MERGE su DATA_ESATTA:
    - Se la data esiste già → aggiorna EXCHANGE_RATE e UPDATED_AT
    - Se la data è nuova → inserisce la riga
*/

WITH staging AS (

    SELECT
        data_esatta,
        base_currency,
        exchange_rate,
        currency,
        loaded_at

    FROM {{ ref('stg_exchange_rate') }}

    {% if is_incremental() %}
        WHERE data_esatta NOT IN (
            SELECT data_esatta FROM {{ this }} WHERE exchange_rate IS NOT NULL
        )
        OR data_esatta IN (
            SELECT s.data_esatta
            FROM {{ ref('stg_exchange_rate') }} s
            JOIN {{ this }} t ON s.data_esatta = t.data_esatta
            WHERE s.exchange_rate != t.exchange_rate
        )
    {% endif %}

)

SELECT
    data_esatta,
    base_currency,
    exchange_rate,
    currency,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS updated_at

FROM staging
