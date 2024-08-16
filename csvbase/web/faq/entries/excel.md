<!--
title = "How do I use csvbase with Microsoft Excel?"
description = "Loading csvbase tables into Excel via Power Query"
draft = false
created = 2024-08-10
updated = 2024-08-10
category = "tools"
-->

## Loading data

1. On the Ribbon, go to the **Data** tab, then select **From web**

![](/static/faq/excel-01-from-web.png)

2. Enter in the table url, with `.xlsx` added on the end and click "Ok".

![](/static/faq/excel-02-from-web-dialog.png)

3. Check the preview and click "Load data"

![](/static/faq/excel-03-navigator.png)

This will load the table as a separate sheet in your workbook.

You can refresh the table from csvbase by clicking **Refresh** on the tab on
the Ribbon.

![](/static/faq/excel-04-refresh.png)

Or, alternatively, you can configure the frequency of the refresh with the
connection properties:

![](/static/faq/excel-06-connection-properties.png)

This allows you to create spreadsheets that always stay up to date.

## Sheet naming conventions

Excel enforces some limitations on sheet names (`/` is not allowed) so csvbase
names the sheets within the XLSX format by the following convention:

`<username>;<table_name>`

And they are truncated if they exceed 31 characters.

## Size limits

Excel does not support sheets longer than 1,048,576 rows so such tables are not
available in XLSX format.

## Writing data from Excel

csvbase supports writing data back, but as far as we know Excel does not.  If
you know of a way, please [open a bug report on
github](https://github.com/calpaterson/csvbase/issues).
