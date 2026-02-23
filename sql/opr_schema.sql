create table if not exists sales(
	customerId bigint, 
	productId bigint, 
	qty bigint,
	updated_at date
);
create table if not exists customers(
	id bigint primary key,
	name text,
	country text,
	updated_at date
);	
create table if not exists products (
	id bigint primary key,
	name text,
	groupname text,
	updated_at date
);
commit;

insert into customers(id, name, country, updated_at)
values(1010, 'CUSTOMER10', 'France', '2026-02-18 00:00:00'),
	  (1020, 'CUSTOMER20', 'Bhutan', current_date+interval '1 day'),
	  (1030, 'CUSTOMER30', 'Chile', current_date),
	  (1040, 'CUSTOMER40', 'Chile', current_date),
	  (1050, 'CUSTOMER5O', 'Chile', null),
	  (1060, 'CUSTOMER60', 'Albania', current_date-interval '1 day'),
	  (1070, 'CUSTOMER70', 'Albania', current_date-interval '1 day'),
	  (1080, 'CUSTOMER80', 'Albania', null);

insert into products(id, name, groupname, updated_at)
values(1100, 'PRODUCT100', 'VIP', current_date-interval '1 day'),
	(1200,   'PRODUCT200', 'VIP', current_date+interval '1 day'),
	(1300,   'PRODUCT300', 'VIP', current_date),
	(1400,   'PRODUCT400', 'VIP', null),
	(1500,   'PRODUCT500', 'VIP', null),
	(1600,   'PRODUCT600', 'TOP', '2026-02-18 00:00:00'),
	(1700,   'PRODUCT700', 'TOP', '2026-02-19 23:59:59'),
	(1800,   'PRODUCT800', 'TOP', current_date);

insert into sales(customerId, productId, qty, updated_at)
values(1010, 1100, 1,current_date-interval '1 day'),
	(1020, 1200, 1,  current_date+interval '1 day'),
	(1030, 1300, 30, current_date),
	(1040, 1400, 2,  null),
	(1050, 1500, 10, null),
	(1060, 1600, 1,  current_date-interval '1 day'),
	(1070, 1700, 1,  current_date),
	(1080, 1800, 1,  current_date);

commit;