<!--
title = "How does csvbase-client's cache work?"
description = "Avoiding pointless redownloads of unchanged tables"
draft = true
created = 2024-05-24
updated = 2024-05-24
-->

The cache built into csvbase-client caches downloaded files, usually in
`~/.cache/csvbase-client`.

Before returning any data to you, the client first checks whether the cached
data is up-to-date with the server, so no stale data is ever returned from the
cache.

The size of the cache is currently limited at 100mb.

You can see the contents via:

```bash
csvbase-client cache show
```

And you can wipe the cache with

```
csvbase-client cache wipe
```
