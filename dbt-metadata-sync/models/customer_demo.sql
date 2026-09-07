{{ config(
    materialized='table',
    alias='customer'
) }}

select
    'C101' as customer_id,
    'MY_CUSTOMER_NAME' as customer_name,
    '123456789' as phone_number