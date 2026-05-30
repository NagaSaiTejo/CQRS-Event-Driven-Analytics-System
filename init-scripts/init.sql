-- Write Model Tables
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  stock INT NOT NULL
);

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL,
  total DECIMAL(10, 2) NOT NULL,
  status VARCHAR(50) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
  id SERIAL PRIMARY KEY,
  order_id INT NOT NULL REFERENCES orders(id),
  product_id INT NOT NULL REFERENCES products(id),
  quantity INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  topic VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  published_at TIMESTAMP NULL
);

-- Read Model Views/Tables (Populated by Consumer)
CREATE TABLE product_sales_view (
  product_id INT PRIMARY KEY,
  total_quantity_sold INT NOT NULL DEFAULT 0,
  total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  order_count INT NOT NULL DEFAULT 0
);

CREATE TABLE category_metrics_view (
  category_name VARCHAR(255) PRIMARY KEY,
  total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  total_orders INT NOT NULL DEFAULT 0
);

CREATE TABLE customer_ltv_view (
  customer_id INT PRIMARY KEY,
  total_spent DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  order_count INT NOT NULL DEFAULT 0,
  last_order_date TIMESTAMP NULL
);

CREATE TABLE hourly_sales_view (
  hour_timestamp TIMESTAMP PRIMARY KEY,
  total_orders INT NOT NULL DEFAULT 0,
  total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

-- For idempotency and sync status tracking
CREATE TABLE processed_events (
  id UUID PRIMARY KEY,
  processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE sync_status (
  id INT PRIMARY KEY DEFAULT 1,
  last_processed_event_timestamp TIMESTAMP NULL
);

INSERT INTO sync_status (id, last_processed_event_timestamp) VALUES (1, NULL) ON CONFLICT DO NOTHING;
