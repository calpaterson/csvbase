<!--
title = "What is a 'table URL'?"
description = "And what can you do with it?"
draft = false
created = 2024-08-20
updated = 2024-08-20
category = "basics"
order = 2
-->

Every table on csvbase has a url like the following:

    https://csvbase.com/<username>/<table_name>

For example:

    https://csvbase.com/meripaterson/stock-exchanges

## Getting alternate formats

To get the file in another format, you just add the relevant file extension to
the table url.  For Excel:

```
https://csvbase.com/<username>/<table_name>.xlsx
```

For example:

```
https://csvbase.com/meripaterson/stock-exchanges.xlsx
```

Several different [formats](formats) are supported.

## Naming restrictions

Table names are only allowed to contain the character, A-Z, a-z and - and they
must start with a letter.

In short they must match the regex:

```
[A-Za-z][-A-Za-z0-9]+
```

## Table API

Each table url supports various different verbs that allow you to [use it as an
API](table-api).
