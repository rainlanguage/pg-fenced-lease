# Security policy

This crate decides which instance may write. A bug that lets two holders
write, or lets a stale holder's write commit, is a security issue.

## Reporting

Please report it privately through
[GitHub private vulnerability reporting](https://github.com/rainlanguage/pg-fenced-lease/security/advisories/new),
not in a public issue. Include the Postgres version, the isolation level,
and a sequence of steps or a test that shows the problem.

We aim to answer within three working days.

## Supported versions

Only the latest released version gets fixes.
