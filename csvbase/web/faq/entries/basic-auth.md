<!--
title = "Do I use an API key?  How do I authenticate?"
description = "HTTP Basic Auth and .netrc"
draft = false
created = 2024-08-09
updated = 2024-08-09
category = "basics"
-->

## When an API key is needed and when it one not

Most usage of csvbase will never need an API key.

If you are just using `GET` on public tables there is no need to supply an API
key.  You do not even have to register.

If you are doing write operations - eg `PUT` (create/overwrite), `POST`
(append) or `DELETE` (self-explanatory) then you do need to provide an API key.

If the table is *private* then even `GET` requests require an API key.

## Your API key

Your API key is on your user page - `https://csvbase.com/<your-username>` (but visible only to you).

It's a 32 character string and looks like this: `0123456789abcdef0123456789abcdef`.

## Basic Auth

csvbase authentication is done via HTTP Basic Auth.

Your username is your username.

Your **API Key** is your password.  Please do not put your site password in the
password field.

Usually, tools that you use allow you to supply a table URL that includes the
username and password.  For example:

`https://calpaterson:0123456789abcdef0123456789abcdef@csvbase.com/calpaterson/countries`

Other times they will not and there will often be a separate field

## The `~/.netrc` file

On unix systems, you can put your username and API key into your `~/.netrc`
file.  Many tools will pick up credentials in this file and use them
automatically.

Curl will use credentials from this file if you pass the `-n` argument.

Here is a sample file:

```ini
machine csvbase.com
  login calpaterson
  password 0123456789abcdef0123456789abcdef
```
