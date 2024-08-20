<!--
title = "What is the table API?"
description = "And what can you do with it?"
draft = false
created = 2024-08-20
updated = 2024-08-20
category = "basics"
order = 4
-->

csvbase has a REST API.

Each [table URL](table-url) doubles as an API endpoint.

## Verbs

- `GET` retrieves the table
  - set the `Accept` header to determine the [format](formats)
- `PUT` creates a new table at `/<your-username>/<some-table-name>`
- `DELETE` deletes the table
- `POST` appends new rows

## Authentication

Authentication isn't required for all requests but is required:
- for private tables (available to [supporters](/billing/pricing)
- and when using the verb `PUT`, `DELETE` or `POST`

Use HTTP [basic auth](basic-auth) to provide your username and API key.

## `csvbase_row_id` and id collation

All csvbase tables contain a column (which is added automatically if not
present) named `csvbase_row_id`.

This column contains autoincremented, unique integers that are used to refer to
specific rows both from outside of csvbase and internally.

If this a value in this row is null, a new, unique row id will be generated.
