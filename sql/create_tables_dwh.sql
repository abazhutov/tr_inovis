create table if not exists dwh_dim_customers(
	id bigint primary key,
	name text,
	country text,
	updated_at date
);
create table if not exists dwh_dim_products (
	id bigint primary key,
	name text,
	groupname text,
	updated_at date
);
create table if not exists dwh_high_water_mark (
    table_name text primary key,
    last_updated date
);
create table if not exists dwh_fact_sales (	
	customerId bigint references dwh_dim_customers(id), 
	productId bigint references dwh_dim_products(id),
	qty bigint,
	updated_at date
);
commit;
create unique index if not exists idx_dwh_fact_sales_customer_product ON dwh_fact_sales(customerId, productId);
commit;