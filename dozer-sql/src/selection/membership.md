# Streaming IN subqueries

The SQL builder lowers uncorrelated `IN (SELECT ...)` and `NOT IN (SELECT ...)`
predicates in `WHERE` into a multi-input membership processor. The outer input
uses the default port; every inner query has a separate numbered port.

The processor retains outer rows, including duplicates and their lifetime
metadata. Inner values are reference-counted, so an inner duplicate neither
duplicates output nor retracts it until its final reference is deleted. Updates
on either side emit the corresponding inserts, updates or deletions. Batch
inserts are supported. Failed operations roll back retained state before any
output is forwarded.

Single outer-row changes evaluate before mutating retained state. Rollback
snapshots are needed only for outer batches and inner-input changes; ordinary
outer inserts do not copy the complete retained collection. Inner rescans clone
one row at a time rather than cloning all retained rows together.

Membership uses existing SQL equality coercion for non-null values. A matching
value wins over NULL; otherwise NULL in a nonempty set makes membership unknown.
The empty set returns false for IN and true for NOT IN, including a NULL probe.
Boolean combinations preserve unknown until the final WHERE filtering step.
Internal membership columns never appear in the output schema or output rows.

Multiple predicates, nested subqueries, CTEs, and the inner query operations
already supported by the streaming planner can be composed. Inner CTE names do
not escape to the parent query. Subqueries must return exactly one column.
Correlated references are not supported; known source qualifiers are validated
rather than silently rebound to an inner column with the same name. Subqueries
inside unsupported scalar-expression forms remain unsupported. This does not
add projected subqueries, ORDER BY, or LIMIT support.

This implementation prioritizes correctness over indexing: an inner change
rescans retained outer rows, and membership scans distinct inner values. State
is in-memory, like the existing join processors; no new persistence or bounded
memory guarantee is introduced.

Tests:

```sh
cargo test -p dozer-sql --lib --locked
cargo test -p dozer-sql-expression --lib --locked
cargo test -p dozer-sql --lib demo_streaming_membership --locked -- --nocapture
```

The demo test shows actual membership-processor operations. The DAG tests also
run parsed SQL through real sources, processors and a recording sink; they check
outer multiplicity and that internal columns do not leak.
