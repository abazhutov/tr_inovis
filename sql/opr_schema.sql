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

insert into customers(id, name, country)
values(1010, 'CUSTOMER10', 'VIP'),
	(1020, 'CUSTOMER20', 'VIP'),
	(1030, 'CUSTOMER30', 'VIP'),
	(1040, 'CUSTOMER40', 'VIP'),
	(1050, 'CUSTOMER50', 'VIP'),
	(1060, 'CUSTOMER60', 'TOP'),
	(1070, 'CUSTOMER70', 'TOP'),
	(1080, 'CUSTOMER80', 'TOP');

insert into products(id, name, groupname)
values(1100, 'PRODUCT100', 'France'),
	(1200, 'PRODUCT200', 'Bhutan'),
	(1300, 'PRODUCT300', 'Chile'),
	(1400, 'PRODUCT400', 'Chile'),
	(1500, 'PRODUCT500', 'Chile'),
	(1600, 'PRODUCT600', 'Albania'),
	(1700, 'PRODUCT700', 'Albania'),
	(1800, 'PRODUCT800', 'Albania');

insert into sales(customerId, productId, qty)
values(1010, 1100, 1),
	(1020, 1200, 1),
	(1030, 1300, 30),
	(1040, 1400, 2),
	(1050, 1500, 10),
	(1060, 1600, 1),
	(1070, 1700, 1),
	(1080, 1800, 1);

commit;