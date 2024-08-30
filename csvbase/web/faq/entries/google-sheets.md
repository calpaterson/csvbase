<!--
title = "How do I use csvbase with Google sheets?"
description = "Loading csvbase tables into Google sheets"
draft = false
created = 2024-08-30
updated = 2024-08-30
category = "tools"
-->

## Loading data

Google Sheets provides the special function
[`IMPORTDATA`](https://support.google.com/docs/answer/3093335) which allows
loading csv files from urls.  Use it like this:

```
=IMPORTDATA("https://csvbase.com/meripaterson/stock-exchanges.csv")
```

![screenshot of IMPORTDATA](/static/faq/google-sheets-importdata.png)

This function loads the data into the cells below (and to the right of) the
cell in which it is entered:

![screenshot of a csvbase table loaded in google sheets](/static/faq/google-sheets-table.png)

Depending on the permissions on your Google sheet, you may be prompted to click
though a dialog box to allow access to external sources:

![screenshot of a permissions-check dialog box](/static/faq/google-sheets-permissions.png)

## Dates

Unfortunately Google Sheets does not parse ISO dates correctly from csv files
(a very similar issue to Excel).

This is very easily fixed by setting the cell format as so:

![screenshot of setting the cell formatting to "Date"](/static/faq/google-sheets-date-fix.png)

Resulting in dates appearing correctly in the sheet:

![screenshot of corrected dates](/static/faq/google-sheets-dates-fixed.png)

## Update frequency

Data loaded with `IMPORTDATA` will [update every hour](https://support.google.com/docs/answer/58515?hl=en#zippy=%2Cchoose-how-often-formulas-calculate).

## Writing data from Google Sheets

This is currently not possible.
