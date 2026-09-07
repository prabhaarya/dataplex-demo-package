{{ config(
    materialized='table'
) }}

SELECT
    'rcpt_982347-4' as receipt_id,
    CURRENT_TIMESTAMP() as timestamp,
    '+441234567890' as phone_number,
    'DELIVERED' as status,
    CAST(NULL AS INT64) as error_code
