<!--
title = "How do I use csvbase with DuckDB?"
description = "Using the plain old HTTPS, or the csvbase-client, to get data into and out of DuckDB"
draft = true
created = 2024-05-23
updated = 2024-05-23
-->

## Read-only access inside the `duckdb` shell

When using the DuckDB shell, you can read any table from csvbase with a line like the following:

```sql
select * from read_parquet("https://csvbase.com/meripaterson/stock-exchanges.parquet");
```

The above uses csvbase's Parquet output format, which works will with DuckDB,
but you can also use csv:

```sql
select * from read_csv_auto("https://csvbase.com/meripaterson/stock-exchanges.csv");
```

## Read and write access from the `duckdb` Python driver

If you're using the Python driver for duckdb you can also use `csvbase-client`
to write back to csvbase.

```bash
# install duckdb and the csvbase-client
pip install duckdb csvbase-client
```

```python
import duckdb, fsspec

# teach DuckDB the csvbase:// url scheme
duckdb.register_filesystem('csvbase')

# create a duckdb table called "stock_exchanges"
duckdb.sql("create table stock_exchanges as from read_csv_auto('csvbase://meripaterson/stock-exchanges')")

# write that local duckdb table back to my own csvbase account as a public table
duckdb.sql("copy stock_exchanges to 'csvbase://calpaterson/duckdb-example?public=true' (HEADER, DELIMITER ',')")
```

Note the following:

1. To avoid accidents, tables are private by default, so add `?public=true`
   when first posting to create a public table
2. Currently the csvbase-client [works only with csv, not
   parquet](https://github.com/calpaterson/csvbase-client/issues/1)
