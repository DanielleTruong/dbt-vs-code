-- created_at: 2025-11-29T18:05:14.456023+00:00
-- finished_at: 2025-11-29T18:05:14.536422+00:00
-- elapsed: 80ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1c1
-- desc: execute adapter call
show terse schemas in database raw
    limit 10000
/* {"app":"dbt","dbt_version":"2.0.0","profile_name":"default","target_name":"dev","connection_name":""} */;
-- created_at: 2025-11-29T18:05:14.767279+00:00
-- finished_at: 2025-11-29T18:05:14.895730+00:00
-- elapsed: 128ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1c5
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "RAW"."RAW" LIMIT 10000;
-- created_at: 2025-11-29T18:05:15.033803+00:00
-- finished_at: 2025-11-29T18:05:15.135780+00:00
-- elapsed: 101ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1cd
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "RAW"."RAW" LIMIT 10000;
-- created_at: 2025-11-29T18:05:14.900310+00:00
-- finished_at: 2025-11-29T18:05:15.145349+00:00
-- elapsed: 245ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1c9
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
-- created_at: 2025-11-29T18:05:15.141734+00:00
-- finished_at: 2025-11-29T18:05:15.347230+00:00
-- elapsed: 205ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1d1
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
-- created_at: 2025-11-29T18:05:15.352411+00:00
-- finished_at: 2025-11-29T18:05:16.688727+00:00
-- elapsed: 1.3s
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.dim_customers
-- query_id: 01c0b85d-3203-81a6-0000-0006e462a1d5
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
