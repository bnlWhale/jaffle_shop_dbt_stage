select
    id as payment_id,
    orderid as order_id,
    paymentmethod,
    amount

from {{ source('jaffle_shop_1', 'payment') }}