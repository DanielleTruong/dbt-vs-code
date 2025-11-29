-- created_at: 2025-11-29T16:39:32.924403+00:00
-- finished_at: 2025-11-29T16:39:33.015852+00:00
-- elapsed: 91ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c0b807-3203-81a6-0000-0006e462a151
-- desc: execute adapter call
show terse schemas in database raw
    limit 10000
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","connection_name":""} */;
-- created_at: 2025-11-29T16:39:33.220338+00:00
-- finished_at: 2025-11-29T16:39:33.354584+00:00
-- elapsed: 134ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c0b807-3203-81a6-0000-0006e462a155
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "RAW"."RAW" LIMIT 10000;
-- created_at: 2025-11-29T16:39:33.447780+00:00
-- finished_at: 2025-11-29T16:39:33.534150+00:00
-- elapsed: 86ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c0b807-3203-81a6-0000-0006e462a15d
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "RAW"."RAW" LIMIT 10000;
-- created_at: 2025-11-29T16:39:33.359838+00:00
-- finished_at: 2025-11-29T16:39:33.537321+00:00
-- elapsed: 177ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c0b807-3203-81a6-0000-0006e462a159
-- desc: execute adapter call
create or replace   view raw.raw.stg_jaffle_shop_orders
  
   as (
     select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from raw.jaffle_shop.orders
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","node_id":"model.jaffle_shop.stg_jaffle_shop_orders"} */;
-- created_at: 2025-11-29T16:39:33.540489+00:00
-- finished_at: 2025-11-29T16:39:33.809008+00:00
-- elapsed: 268ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c0b807-3203-81a6-0000-0006e462a161
-- desc: execute adapter call
create or replace   view raw.raw.stg_jaffle_shop_customers
  
   as (
     select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers
  )
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","node_id":"model.jaffle_shop.stg_jaffle_shop_customers"} */;
-- created_at: 2025-11-29T16:39:33.813015+00:00
-- finished_at: 2025-11-29T16:39:33.913218+00:00
-- elapsed: 100ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.dim_customers
-- query_id: 01c0b807-3203-81a6-0000-0006e462a165
-- desc: execute adapter call
drop view if exists RAW.RAW.DIM_CUSTOMERS cascade
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","node_id":"model.jaffle_shop.dim_customers"} */;
-- created_at: 2025-11-29T16:39:33.917714+00:00
-- finished_at: 2025-11-29T16:39:35.669817+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.dim_customers
-- query_id: 01c0b807-3203-81a6-0000-0006e462a169
-- desc: execute adapter call
create or replace transient  table raw.raw.dim_customers
    
    
    
    as (
with customers as (

    select * from raw.raw.stg_jaffle_shop_customers
   
),

orders as (

   select * from raw.raw.stg_jaffle_shop_orders

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final
    )

/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","node_id":"model.jaffle_shop.dim_customers"} */;
