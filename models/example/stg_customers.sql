with customers as (
    
    select 
        id as customer_id,
        first_name,
        last_name

    from amos_dbt.customers
)

select * from customers
