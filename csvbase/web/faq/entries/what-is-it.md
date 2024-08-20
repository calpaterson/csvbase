<!--
title = "What is csvbase?"
description = "What is it?"
draft = false
created = 2024-06-10
updated = 2024-08-20
category = "basics"
order = 1
-->

csvbase is a [website for sharing table data](/about).

"Table data" means (labelled) columns and (indexed) rows.

Each table has it's [own url](table-url), following the format:

    https://csvbase.com/<username>/<table_name>

That url serves both as the web page for the table, and also for [it's
API](/table-api).

csvbase is called *csv*base because one of the easiest ways to interact with it
is via csv files.  To get the csv file for any table, just add `.csv` to the
url, so

[https://csvbase.com/meripaterson/stock-exchanges](https://csvbase.com/meripaterson/stock-exchanges)

becomes

<a href="https://csvbase.com/meripaterson/stock-exchanges.csv">https://csvbase.com/meripaterson/stock-exchanges<strong>.csv</strong></a>

Many other [formats are available](formats), including:
- JSON
- Parquet
- Microsoft Excel
