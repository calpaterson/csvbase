<!--
title = "How do I use csvbase with DuckDB?"
description = "Using HTTPS, or the csvbase-client, to import/export csvbase tables with DuckDB"
draft = false
created = 2024-05-23
updated = 2024-05-23
category = "tools"
-->

## Reading csvbase tables into the `duckdb` shell

When using the DuckDB shell (ie: you've run `duckdb`), you can read any table
from csvbase with a line like the following:

```sql
select * from read_parquet("https://csvbase.com/meripaterson/stock-exchanges.parquet");
```

The above uses DuckDB's [httpfs](https://duckdb.org/docs/extensions/httpfs/overview.html) extension.  It also uses csvbase's Parquet output format, which works well with DuckDB.

You can also use plain csv:

```sql
select * from read_csv_auto("https://csvbase.com/meripaterson/stock-exchanges.csv");
```

But that is, generally, slower.

## Read and write access from the `duckdb` Python driver

If you're using the Python driver for duckdb you can also use `csvbase-client`
to write back to csvbase.

First, install both duckdb ([the Python
library](https://pypi.org/project/duckdb/)) and
[csvbase-client](https://pypi.org/project/csvbase-client/).

```bash
# install duckdb and the csvbase-client
pip install duckdb csvbase-client
```

Then, in Python:

```python
import duckdb, fsspec

# teach DuckDB the csvbase:// url scheme
duckdb.register_filesystem(fsspec.filesystem('csvbase'))

# create a duckdb table called "stock_exchanges"
duckdb.sql("""
    CREATE TABLE stock_exchanges
    AS FROM read_csv_auto('csvbase://meripaterson/stock-exchanges')
""")

# write that local duckdb table back to my own csvbase account as a public table
duckdb.sql("""
    COPY stock_exchanges TO
    'csvbase://calpaterson/duckdb-example?public=true' (HEADER, DELIMITER ',')
""")
```

Note the following:

1. To avoid accidents, tables are created as private by default, so add
   `?public=true` when first posting to create a public table
2. Currently the csvbase-client [works only with csv, not
   parquet](https://github.com/calpaterson/csvbase-client/issues/1)
