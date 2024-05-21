<!--
title = "How to do I use csvbase with data stored in git?"
description = "Using csvbase as a web frontend to csv files in git"
draft = false
created = 2024-05-21
updated = 2024-05-21
-->

csvbase can provide read (and write) access to csv files stored in a git repo.

![screenshot of a csvbase table tracking a github
file](/static/faq/csvbase-tracking-table.png)

When you edit a git-linked csvbase table (via the API, via the website,
however) that change will be commited and pushed to your git repo.

To create a table with a git upstream, visit the [new table (from
git)](/new-table/git) page and fill out the form.

If your github repo is public and read-only access is all you need, that's it.
However if your repo is private or you want to be able to edit your data on
csvbase you will need to provide authentication in your repository url, for
example as:

`https://calpaterson:github_pat_ABCD1234@github.com/calpaterson/csvbase.git`

To find out how to generate a suitable personal access token for github, [see
that FAQ](/github-pat).
