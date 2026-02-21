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
	customerId bigint references stg_dim_customers(id), 
	productId bigint references stg_dim_products(id), 
	qty bigint,
	updated_at date
);
commit;
create unique index if not exists idx_stg_fact_sales_customer_product ON stg_fact_sales(customerId, productId);
commit;