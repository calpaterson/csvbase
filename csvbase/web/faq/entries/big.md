<!--
title = "When are tables 'big'?"
description = "What counts as 'big'?  What is different about big tables?"
draft = false
created = 2024-07-02
updated = 2024-07-02
-->

## When is a table considered big on csvbase?

csvbase considers a table as "big" if it has more than 1,048,576 rows.

## What is different about big tables?

You need to be patient when working with big tables!

Big tables are still supported but many operations on them happen
asynchronously.  For example, you might have to wait before downloading the
table in a certain file format (while csvbase generates it for you).

Occasionally, csvbase might return the HTTP status code 503 for things to do
with big tables.  That doesn't mean that anything is broken, only the csvbase
is still working on it.

The HTTP header `Retry-After` will be set to give API clients a hint of how
long to wait before retrying the request.

## Why 1,048,576 rows?

There is no agreed definition of "Big Data" but one useful definition is "too
big for Excel".  Microsoft Excel imposes a hard limit of 1,048,576 rows per
sheet.
