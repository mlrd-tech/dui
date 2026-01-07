# dui ~ Terminal UI for DynamoDB Local

Kind of like the `mysql` shell for MySQL, but for DynamoDB local.
By design, it (probably) doesn't work with real DynamoDB tables to prevent mistakes and money.

Why?
Because development is messy and so are schemaless tables.

Just `go build` and run `dui`.
It connects to `http://localhost:8000` (DynamoDB local) by default.

Deeply satisfying Vim-like keyboard shorts, like `dd` to delete an item.
Also `\q` to quit because, you know... MySQL.

**This is only a dev tool; do not use it in production.**
Expect bugs and random changes.
PRs are welcome.
