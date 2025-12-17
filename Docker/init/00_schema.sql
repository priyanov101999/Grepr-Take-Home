-- Business tables (what users can query)
create table if not exists customers (
  id bigint primary key,
  email text not null,
  name text not null,
  created_at timestamptz not null default now()
);

create table if not exists products (
  id bigint primary key,
  sku text not null,
  name text not null,
  price_cents int not null,
  created_at timestamptz not null default now()
);

create table if not exists orders (
  id bigint primary key,
  customer_id bigint not null references customers(id),
  status text not null,
  order_total_cents int not null,
  created_at timestamptz not null default now()
);

create table if not exists order_items (
  id bigint primary key,
  order_id bigint not null references orders(id),
  product_id bigint not null references products(id),
  qty int not null,
  unit_price_cents int not null
);

create index if not exists orders_customer_id_idx on orders(customer_id);
create index if not exists order_items_order_id_idx on order_items(order_id);
create index if not exists order_items_product_id_idx on order_items(product_id);

-- Metadata table (query tracking)
create table if not exists queries (
  id text primary key,
  user_id text not null,
  idempotency_key text,
  sql text not null,
  status text not null,
  created_at timestamptz not null,
  started_at timestamptz,
  ended_at timestamptz,
  error text,
  result_path text,
  rows_written bigint not null default 0,
  bytes_written bigint not null default 0
);

create unique index if not exists queries_user_id_idem_idx
  on queries(user_id, idempotency_key)
  where idempotency_key is not null;

create index if not exists queries_user_status_idx on queries(user_id, status);
