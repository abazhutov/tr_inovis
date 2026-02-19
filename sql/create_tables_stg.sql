
create table if not exists stg_dim_customers(
	id bigint primary key,
	name TEXT,
	country TEXT,
	updated_at TIMESTAMP
);
create table if not exists stg_dim_products (
	id bigint primary key,
	name TEXT,
	groupname TEXT,
	updated_at TIMESTAMP
);
create table if not exists stg_fact_sales (
	customerId bigint references stg_dim_customers(id), 
	productId bigint references stg_dim_products(id), 
	qty bigint,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);
commit;