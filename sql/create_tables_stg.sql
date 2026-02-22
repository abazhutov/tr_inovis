create table if not exists stg_dim_customers(
	id bigint primary key,
	name text,
	country text,
	updated_at date
);
create table if not exists stg_dim_products (
	id bigint primary key,
	name text,
	groupname text,
	updated_at date
);
create table if not exists stg_fact_sales (
	customer_id bigint, 
	product_id bigint, 
	qty bigint,
	updated_at date
);
commit;