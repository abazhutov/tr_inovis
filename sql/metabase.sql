#count sales by country
select c.country, sum(s.qty) as count   
from dwh_fact_sales s
join dwh_dim_customers c on s.customer_id = c.id
-- join dwh_dim_products p on s.product_id = p.id
group by 1

-- avg sales by product for last 30 days
select p.name, avg(s.qty) as avg 
from dwh_fact_sales s
-- join dwh_dim_customers c on s.customer_id = c.id
join dwh_dim_products p on s.product_id = p.id
where s.updated_at > current_date - interval '30 day'
group by 1

-- data model star
select p.name as product_name, c.name as customer_name, country, groupname, qty as quantity
from dwh_fact_sales s
join dwh_dim_customers c on s.customer_id = c.id
join dwh_dim_products p on s.product_id = p.id