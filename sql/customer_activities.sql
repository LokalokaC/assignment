CREATE SCHEMA IF NOT EXISTS business;

CREATE TABLE IF NOT EXISTS business.customer_activities (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    activity_date DATE NOT NULL,
    customer_id TEXT,
    activity TEXT,
    _created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (timestamp, customer_id, activity)
);

CREATE INDEX IF NOT EXISTS _customer_id ON business.customer_activities (customer_id);
CREATE INDEX IF NOT EXISTS _activity_date ON business.customer_activities (activity_date);