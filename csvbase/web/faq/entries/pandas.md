<!--
title = "How do I use csvbase with Pandas?"
description = "Using HTTPS, or the csvbase-client, to import/export csvbase tables with DuckDB"
draft = false
created = 2024-05-29
updated = 2024-05-29
-->

## Reading csvbase tables into Pandas

When using Pandas, you can read any table by copying the url into Panda's
[`read_csv`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
function.

```python
import pandas as pd

df = pd.read_csv("https://csvbase.com/meripaterson/stock-exchanges", index_col=0)
```

`index_col=0` just makes the csvbase row id the index.

You can also use Parquet if you have the relevant parquet libraries installed
(generally, that means [pyarrow](https://pypi.org/project/pyarrow/)) but it is
best to install that via the parquet "extra":

```bash
pip install pandas[parquet]
```

Then read data with Panda's
[`read_parquet`](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html)
funciton.

```python
df = pd.read_parquet("https://csvbase.com/meripaterson/stock-exchanges.parquet")
```

## Reading and writing via `csvbase-client`

Simple reads are done fine with just Pandas, however if you want to do writes
or benefit from [caching](/faq/csvbase-client-cache) it is best to use the
`csvbase-client` library.

```bash
pip install csvbase-client
```

Then you can do reads using the `csvbase://` url scheme (you do not need to
import anything - Pandas will pick it up automatically):

```python
df = pd.read_csv("csvbase://meripaterson/stock-exchanges")
```

Writes are done the same way:

```python
df.to_csv("csvbase://myuser/stock-exchanges")
```
