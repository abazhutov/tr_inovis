#fact_sales
insert into mrr_fact_sales (customerId, productId, qty, created_at, updated_at)
-- VALUES(%s, %s, %s, %s, %s)
VALUES(1, 1, 10, now(), now())
on CONFLICT (customerId, productId) DO UPDATE SET
qty = EXCLUDED.qty,
created_at = EXCLUDED.created_at,
updated_at = EXCLUDED.updated_at;