create table if not exists dwh_fact_sales (
	customerId bigint, 
	productId bigint, 
	qty bigint,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);
create table if not exists dwh_dim_customers(
	id bigint primary key,
	name TEXT,
	country TEXT,
	updated_at TIMESTAMP
);
create table if not exists dwh_dim_products (
	id bigint primary key,
	name TEXT,
	groupname TEXT,
	updated_at TIMESTAMP
);
create table if not exists dwh_high_water_mark (
    table_name TEXT primary key,
    last_updated TIMESTAMP
);