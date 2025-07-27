
CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.orders (
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    region TEXT,
    product_category TEXT,
    product_name TEXT,
    quantity INTEGER NOT NULL DEFAULT 0,
    unit_price NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    tax_amount NUMERIC(10, 2),
    shipping_cost NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    payment_method TEXT,
    _created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, timestamp)
);

CREATE INDEX IF NOT EXISTS _order_id ON business.orders (order_id);
CREATE INDEX IF NOT EXISTS _customer_id ON business.orders (customer_id);
CREATE INDEX IF NOT EXISTS _timestamp ON business.orders (timestamp);