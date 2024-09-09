<!--
title = "What Markdown is supported>"
description = "csvbase uses GFM - plus some other bits"
draft = false
created = 2024-09-09
updated = 2024-09-09
-->

csvbase supports Markdown (including some HTML tags).

The specific version of Markdown is ["Github Flavoured
Markdown"](https://github.github.com/gfm/).

## Tables

You can markup tables like this

```markdown
| Column 1 | Column 2 |
| -------- | -------- |
|        a |        1 |
|        b |        2 |
|        c |        3 |
```

See [the GFM specification](https://github.github.com/gfm/#tables-extension-)
for more details and features.

## References

### Comments

You can reference other comments in the same thread like this:

```markdown
I note in passing comment #3
```

Which will be rendered as a permalink to comment #3 in the same thread (and
your comment will be forward-linked on comment #3 as a reply).
