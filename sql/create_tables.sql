create table if not exists sales(
	customerId bigint primary key, 
	productId bigint, 
	qty bigint
);
create table if not exists customers(
	id bigint primary key,
	name TEXT,
	country TEXT
);	
create table if not exists products (
	id bigint primary key,
	name TEXT,
	groupname TEXT
);