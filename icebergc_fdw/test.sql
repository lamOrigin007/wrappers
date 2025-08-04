-- SQL tests for icebergc_fdw
CREATE EXTENSION IF NOT EXISTS icebergc_fdw;

CREATE SERVER iceberg_srv FOREIGN DATA WRAPPER icebergc_fdw
OPTIONS (
    catalog_uri '/tmp/data.parquet'
);

CREATE FOREIGN TABLE iceberg_tbl (
    id integer,
    name text,
    price float8,
    active boolean,
    created_at timestamp
) SERVER iceberg_srv;

-- Basic query with WHERE, ORDER BY and LIMIT
SELECT * FROM iceberg_tbl WHERE id > 10 ORDER BY price LIMIT 5;

-- Query checking boolean filter and ordering
SELECT id, name FROM iceberg_tbl WHERE active = true ORDER BY id;

-- Query testing various data types
SELECT id, price, active, created_at FROM iceberg_tbl ORDER BY created_at DESC LIMIT 3;
