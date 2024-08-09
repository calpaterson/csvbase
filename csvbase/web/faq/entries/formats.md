<!--
title = "What formats does csvbase support?"
description = "Accessing tables in various formats"
draft = false
created = 2024-05-21
updated = 2024-05-21
category = "basics"
-->

csvbase supports the following formats:

| Format name                                                                             | File extension | HTTP content type                                                   | Paged |
|-----------------------------------------------------------------------------------------|----------------|---------------------------------------------------------------------|-------|
| HTML                                                                                    | `.html`        | `text/html`                                                         | Yes   |
| [JSON](https://en.wikipedia.org/wiki/JSON)                                              | `.json`        | `application/json`                                                  | Yes   |
| CSV ([Comma separated variables](https://en.wikipedia.org/wiki/Comma-separated_values)) | `.csv`.        | `text/csv`                                                          | No    |
| [Parquet](https://en.wikipedia.org/wiki/Apache_Parquet)                                 | `.parquet`     | `application/parquet` [non-standard]                                | No    |
| [JSON lines](https://jsonlines.org/)                                                    | `.jsonl`       | `application/x-jsonlines` [non-standard]                            | No    |
| [Microsoft Excel](https://en.wikipedia.org/wiki/Office_Open_XML)                        | `.xlsx`        | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | No    |

To download a table in a given format, you have three options:
1. Use the dropdown in the web UI
2. Append the file extension to the url
   - eg [csvbase.com/meripaterson/stock-exchanges.**xlsx**](https://csvbase.com/meripaterson/stock-exchanges.xlsx)
3. Set the HTTP Accept header (in your HTTP client)
   - eg `curl -H 'Accept: application/x-jsonlines' https://csvbase.com/meripaterson/stock-exchanges`

For "paged" formats (like JSON) you will need to go through the dataset page by
page to read all of it.  For unpaged formats (like Parquet or Microsoft Excel)
you will just recieve the entire table as a single file.
