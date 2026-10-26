{{ config(materialized='view') }}
select * from read_parquet('{{ env_var("SILVER_PARQUET") }}')
