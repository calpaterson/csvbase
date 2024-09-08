<!--
title = "How do I choose my avatar?"
description = "csvbase uses Gravatar"
draft = false
created = 2024-09-08
updated = 2024-09-08
-->

csvbase avatars are taken from Gravatar, an avatar hosting system run by
Wordpress.

To set up a Gravatar, visit [gravatar.com](https://gravatar.com/).  Then set
the email you used on Gravatar as your csvbase email in your user settings and
tick "Use my Gravatar" on the same page.

## Disabled by default

Gravatars are supported, but disabled by default as a privacy feature.  This
helps avoid users accidentally exposing a photo of themselves just because they
entered their email address into csvbase.

Additionally, csvbase reverse proxies all requests for Avatars to avoid
disclosing the SHA256 hash of your email address.
