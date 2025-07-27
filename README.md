## Table of Contents

- [Quick Start Guide](#quick-start-guide)
- [Pgadmin UI](#pgadmin-ui)
- [Pgadmin Server](#pgadmin-server)
- [Airflow UI](#airflow-ui)
- [Schema Choices](#schema-choices)
- [Assignments](#assignments)

### Quick Start Guide

1. Clone repository: `git clone https://github.com/LokalokaC/assignment.git`
2. Navigate to project: `cd assignment`
3. Run `start.sh`: `bash start.sh` (make sure Docker is running)  
   > On Windows, use Git Bash or WSL to run shell scripts.
4. Log into [PgAdmin UI](#pgadmin-ui) → Right Click "Server" and navigate to "Register" → "Server"
5. Click "Server" → fill in connection and credentials from [Pgadmin Server](#pgadmin-server)
6. Refer to [Assignments](#assignments) for SQL tasks and examples


---

> **Note:** PostgreSQL is **not** configured with a persistent volume.  
> All database content will be **cleared** upon container shutdown.  
> The Airflow metadata database will be **reinitialized** each time the containers start.

### Pgadmin UI

- URL: [http://localhost:5050](http://localhost:5050)
- Username: `admin@admin.com`
- Password: `admin`

### Pgadmin Server
- host: postgres
- port: 5432
- database: airflow
- user: airflow
- password: airflow

### Airflow UI
- URL: [http://localhost:8080](http://localhost:8080)
- Username: `airflow`
- Password: `airflow`

---
### Schema Choices

1. Table: orders
```sql

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
```

### Reasons
- Primary Key:
    order_id and timestamp are combined to form the primary key. Duplicate order_ids were found with different timestamps, so timestamp was included to uniquely identify each record.

- Timestamp selection:
    - Both order_data_1.csv and order_data_2.csv include a timestamp column. However, the one from order_data_1.csv was used because:
      - The two timestamps do not match exactly at the datetime level.
      - The timestamp from order_data_1.csv aligns precisely with the TransactionDate in order_data_2.csv at the date level.
      - Using the order_data_1.csv timestamp ensures consistency and better reflects the transaction timeline.
  
- Columns kept: 
    - order_id, customer_id, timestamp, region, payment_method: essential for order tracking and customer reporting.
    - quantity, shipping_cost, total_amount, tax_amount: core financial metrics for sales analysis.
    - product_category, product_name, unit_price: included for completeness and can be normalized into a product dimension table if needed.

- Columns dropped: Operational and technical fields were excluded to keep the schema focused on business analytics:
    - SessionID, LogID: user session trackers, not relevant to order-level reporting.
    - ServerNode, CacheHit, IPAddress, ProcessingTime, APIResponseCode, RiskScore: infrastructure-related metadata, not required for analysis.

- Normalization:
    - Values in order_id, customer_id, region, payment_method, product_category, and product_name were uppercased for consistency.
    - Invalid values in shipping_cost, total_amount, and tax_amount were filled with 0. Rows where all of these fields are 0 were removed.
    - Since the two timestamp fields did not match, transaction_date was derived from the date part of the order_data_1.csv timestamp to join with order_data_2.csv.
    - Rows missing either timestamp or order_id were dropped during transformation.


2. Table: customer_activities
```sql
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
```
### Reasons
- Primary Key:
    - The combination of timestamp, customer_id, and activity uniquely identifies each user action. Since a customer may perform multiple activities per day, this combined key avoids deduplication and captures event-level granularity.

- Indexes:
    - activity_date and customer_id are frequently used in queries for DAU, behavioral analysis, and trend reporting.
    - activity is not indexed due to low filtering relevance.

- Columns kept:
    - All three fields were retained to preserve the uniqueness and completeness of user activity logs.
    - activity_date was generated from timestamp for frequent filtering

- Normalization:
    - Values in customer_id and activity are upper-cased for consistency.
    - Rows missing either timestamp or customer_id were removed during transformation to ensure data integrity.

---
### Assignments:

- Task 1 & 2:
```sql
with dau as (
    select
        activity_date,
        count(distinct customer_id) as dau
    from business.customer_activities
    where timestamp >= '2025-01-26' and timestamp < '2025-02-09'
    group by 1
)

select
    case
        when activity_date between '2025-01-26' and '2025-02-01' then 'week_1'
        when activity_date between '2025-02-02' and '2025-02-08' then 'week_2'
    end as week_num,
    round(avg(dau), 2) as avg_weekly_dau
from dau
group by 1
```
- Task 3 & 4:
```sql
select
    case
        when timestamp >= '2025-01-26' and timestamp < date '2025-02-02' then 'week_1'
        when timestamp >= '2025-02-02' and timestamp < date '2025-02-09' then 'week_2'
    end as week_num,
        sum(total_amount) as sales
    from business.orders
    where timestamp >= '2025-01-26' and timestamp < '2025-02-09'
    group by 1
```
