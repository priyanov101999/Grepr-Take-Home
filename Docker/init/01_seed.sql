insert into customers(id, email, name, created_at)
select
  gs,
  'user' || gs || '@example.com',
  'Customer ' || gs,
  now() - (random() * interval '365 days')
from generate_series(1, 100000) gs
on conflict do nothing;

insert into products(id, sku, name, price_cents, created_at)
select
  gs,
  'SKU-' || gs,
  'Product ' || gs,
  (100 + (random() * 9900))::int,
  now() - (random() * interval '365 days')
from generate_series(1, 50000) gs
on conflict do nothing;

insert into orders(id, customer_id, status, order_total_cents, created_at)
select
  gs,
  (1 + (random() * 99999))::bigint,
  (array['PLACED','PAID','SHIPPED','CANCELLED'])[1 + (random()*3)::int],
  (500 + (random() * 50000))::int,
  now() - (random() * interval '180 days')
from generate_series(1, 2000000) gs
on conflict do nothing;

insert into order_items(id, order_id, product_id, qty, unit_price_cents)
select
  gs,
  ((gs + 1) / 2)::bigint,
  (1 + (random() * 49999))::bigint,
  (1 + (random() * 4))::int,
  (100 + (random() * 9900))::int
from generate_series(1, 4000000) gs
on conflict do nothing;

analyze customers;
analyze products;
analyze orders;
analyze order_items;
