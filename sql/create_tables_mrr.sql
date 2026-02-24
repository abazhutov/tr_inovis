create table if not exists mrr_dim_customers(
	id bigint,
	name text,
	country text,
	updated_at date
);
create table if not exists mrr_dim_products (
	id bigint,
	name text,
	groupname text,
	updated_at date
);
create table if not exists mrr_fact_sales (
	customerId bigint, 
	productId bigint, 
	qty bigint,
	updated_at date
);
commit;