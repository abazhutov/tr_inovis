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
create table if not exists high_water_mark (
    table_name text primary key,
    last_update date
);

insert into high_water_mark (table_name, last_update) 
values ('sales', '1900-01-01'),
	('customers', '1900-01-01'),
	('products', '1900-01-01')
on conflict (table_name) do nothing;
commit;

create table if not exists dwh_fact_sales (	
	customer_id bigint, 
	product_id bigint,
	qty bigint,
	updated_at date
);
commit;
create unique index if not exists idx_dwh_fact_sales_customer_product ON dwh_fact_sales(customer_id, product_id);
commit;

create table if not exists airflow_log (
    dag_id text not null,
    task_id text not null,
    run_id text not null,
    status bool not null,                
    start_time timestamp not null,
    end_time timestamp not null,
    duration_seconds numeric(12,3) not null,
    error_message text,
    created_at timestamp default now()
);
create table if not exists action_log (
    object_name text,
    id bigint,
    start_time timestamp,
    end_time timestamp,
    duration_seconds numeric(12,3),
    error_message text,
    created_at timestamp default now()
);