with customer as (
select * from {{ ref('stg_customers') }}
),
 
nation as (
select * from {{ ref('stg_nations') }}
),
 
region as (
select * from {{ ref('stg_regions') }}
),
final as (
select
        customer.customer_id,
        customer.name,
        customer.address,
        nation.n_nationkey as nation_id,
        nation.n_name as nation,
        region.r_regionkey as region_id,
        region.r_name as region,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment 
        from customer
        inner join nation
            on customer.nation_id = nation.n_nationkey
        inner join region
            on nation.n_regionkey = region.r_regionkey
 
)
select * from final