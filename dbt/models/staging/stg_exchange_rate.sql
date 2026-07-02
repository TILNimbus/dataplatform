{{
  config(
    materialized     = 'incremental',
    unique_key       = 'data_esatta',
    on_schema_change = 'append_new_columns',
    tags             = ['staging', 'exchange_rate']
  )
}}

/*
  stg_exchange_rate.sql
  ─────────────────────────────────────────────────────────────────
  Legge dalla tabella raw append-only RAW.EXCHANGE_RATE (scritta da N8N)
  e produce una vista pulita e deduplicata per il modello marts.

  - Incremental: processa solo le righe nuove dall'ultimo run
  - Deduplicazione: se N8N ha inserito più righe per la stessa data
    (es. retry), mantiene solo il record con LOADED_AT più recente
  - Cast espliciti per garantire compatibilità downstream
*/

WITH source AS (

    SELECT
        DATA_ESATTA::DATE            AS data_esatta,
        BASE_CURRENCY::VARCHAR(10)   AS base_currency,
        EXCHANGE_RATE::FLOAT         AS exchange_rate,
        CURRENCY::VARCHAR(10)        AS currency,
        LOADED_AT::TIMESTAMP_NTZ     AS loaded_at

    FROM {{ source('raw', 'exchange_rate') }}

    {% if is_incremental() %}
        WHERE LOADED_AT > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        data_esatta,
        base_currency,
        exchange_rate,
        currency,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY data_esatta
            ORDER BY loaded_at DESC
        ) AS rn

    FROM source

)

SELECT
    data_esatta,
    base_currency,
    exchange_rate,
    currency,
    loaded_at

FROM deduplicated
WHERE rn = 1
