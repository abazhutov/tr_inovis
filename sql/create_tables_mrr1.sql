create table if not exists mrr_dim_customers(
	id bigint,
	name TEXT,
	country TEXT,
	updated_at TIMESTAMP
);
create table if not exists mrr_dim_products (
	id bigint,
	name TEXT,
	groupname TEXT,
	updated_at TIMESTAMP
);
create table if not exists mrr_high_water_mark (
    table_name TEXT,
    last_updated TIMESTAMP
);

create table if not exists mrr_fact_sales (
	customerId bigint, 
	productId bigint, 
	qty bigint,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);

alter table mrr_dim_customers drop constraint if exists pk_mrr_dim_customers;
alter table mrr_dim_products drop constraint if exists pk_mrr_dim_products;
alter table mrr_high_water_mark drop constraint if exists pk_mrr_high_water_mark;
alter table mrr_fact_sales drop constraint if exists pk_mrr_fact_sales;

alter table mrr_dim_customers add constraint pk_mrr_dim_customers primary key(id);
alter table mrr_dim_products add constraint pk_mrr_dim_products primary key(id);
alter table mrr_high_water_mark add constraint pk_mrr_high_water_mark primary key(id);
alter table mrr_fact_sales add constraint pk_mrr_fact_sales primary key(id);

create unique index if not exists idx_mrr_fact_sales_customer_product ON mrr_fact_sales(customerId, productId);