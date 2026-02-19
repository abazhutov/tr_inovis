-- drop Table if exists mrr_fact_sales;
-- drop Table if exists mrr_dim_customers;
-- drop Table if exists mrr_dim_products;
-- drop Table if exists mrr_high_water_mark;

create table if not exists mrr_dim_customers(
	id bigint primary key,
	name TEXT,
	country TEXT,
	updated_at TIMESTAMP
);
create table if not exists mrr_dim_products (
	id bigint primary key,
	name TEXT,
	groupname TEXT,
	updated_at TIMESTAMP
);
create table if not exists mrr_high_water_mark (
    table_name TEXT primary key,
    last_updated TIMESTAMP
);
create table if not exists mrr_fact_sales (
	customerId bigint, 
	productId bigint, 
	qty bigint,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);
commit;
create unique index if not exists idx_mrr_fact_sales_customer_product ON mrr_fact_sales(customerId, productId);
commit;