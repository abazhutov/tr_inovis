create table if not exists sales(
	customerId bigint, 
	productId bigint, 
	qty bigint,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);
create table if not exists customers(
	id bigint primary key,
	name TEXT,
	country TEXT,
	updated_at TIMESTAMP
);	
create table if not exists products (
	id bigint primary key,
	name TEXT,
	groupname TEXT,
	updated_at TIMESTAMP
);
commit;

insert into customers(id, name, country, updated_at)
values(1010, 'CUSTOMER10', 'VIP', '2026-02-18 00:00:00'),
	  (1020, 'CUSTOMER20', 'VIP', LOCALTIMESTAMP+INTERVAL '1 day'),
	  (1030, 'CUSTOMER30', 'VIP', LOCALTIMESTAMP),
	  (1040, 'CUSTOMER40', 'VIP', LOCALTIMESTAMP),
	  (1050, 'CUSTOMER5O', 'VIP', NULL),
	  (1060, 'CUSTOMER60', 'TOP', LOCALTIMESTAMP-INTERVAL '1 day'),
	  (1070, 'CUSTOMER70', 'TOP', LOCALTIMESTAMP-INTERVAL '1 day'),
	  (1080, 'CUSTOMER80', 'TOP', NULL);

insert into products(id, name, groupname, updated_at)
values(1100, 'PRODUCT100', 'France', LOCALTIMESTAMP-INTERVAL '1 day'),
	(1200,   'PRODUCT200', 'Bhutan', LOCALTIMESTAMP+INTERVAL '1 day'),
	(1300,   'PRODUCT300', 'Chile',  LOCALTIMESTAMP),
	(1400,   'PRODUCT400', 'Chile',  NULL),
	(1500,   'PRODUCT500', 'Chile',  NULL),
	(1600,   'PRODUCT600', 'Albania', '2026-02-18 00:00:00'),
	(1700,   'PRODUCT700', 'Albania', '2026-02-19 23:59:59'),
	(1800,   'PRODUCT800', 'Albania', LOCALTIMESTAMP);

insert into sales(customerId, productId, qty, created_at, updated_at)
values(1010, 1100, 1,LOCALTIMESTAMP-INTERVAL '1 day', LOCALTIMESTAMP-INTERVAL '1 day'),
	(1020, 1200, 1,  LOCALTIMESTAMP+INTERVAL '1 day', LOCALTIMESTAMP+INTERVAL '1 day'),
	(1030, 1300, 30, LOCALTIMESTAMP, LOCALTIMESTAMP),
	(1040, 1400, 2,  NULL, NULL),
	(1050, 1500, 10, NULL, NULL),
	(1060, 1600, 1,  LOCALTIMESTAMP-INTERVAL '1 day', LOCALTIMESTAMP-INTERVAL '1 day'),
	(1070, 1700, 1,  LOCALTIMESTAMP, LOCALTIMESTAMP),
	(1080, 1800, 1,  LOCALTIMESTAMP, LOCALTIMESTAMP);

commit;