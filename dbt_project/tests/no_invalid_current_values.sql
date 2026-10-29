SELECT *
FROM {{ ref('fct_current_trades') }}
WHERE QUANTITY <= 0 OR PRICE <= 0
