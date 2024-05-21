<!--
title = "How do I generate a Github Personal Access Token?"
description = "Creating user/password authentication for your git repo"
draft = false
created = 2024-05-21
updated = 2024-05-21
-->

csvbase can provide read (and write) access to csv files stored in a git repo.

If your repository is private or if you want to be able to edit your data on
csvbase, you will need to provide a ["fine grained personal access
token"](https://github.com/settings/tokens?type=beta).

When [generating a new
token](https://github.com/settings/personal-access-tokens/new) csvbase requires
the following *Repository permissions*:

1. **Contents** - "Repository contents, commits, branches, downloads, releases, and
   merges."
   - **Read & write** allows csvbase to both read the repo, and write to it
2. Webhooks - "Manage the post-receive hooks for a repository."
   - **Read & write** allows csvbase to add webhooks ([in the near
     future](https://github.com/calpaterson/csvbase/issues/125)) to stay up to
     date instantly instead of having to periodically poll for changes
3. **Metadata**
   - **Read-only** is default and mandatory for all tokens.  csvbase does not
     currently have any use for this permission.

![github PAT overview](/static/faq/github-pat-overview.png)
