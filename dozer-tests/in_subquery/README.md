# Streaming IN subqueries

`WHERE value IN (SELECT value FROM allowed)` is a live membership test. Inner inserts can admit already-seen outer rows; deleting the final matching inner value retracts them. `NOT IN` uses SQL three-valued logic: a nonmatching value is unknown while the inner result contains NULL, and an empty inner result makes `NOT IN` true, including for a NULL outer value.

```sql
SELECT * FROM orders
WHERE customer_id IN (
  SELECT customer_id FROM customers WHERE active = 1
);
```

The planner builds the inner SELECT and a two-input mark join. Reference-counted inner values preserve outer multiplicity without multiplying rows by inner duplicates. Same-type membership updates use a hash index; comparisons between different field types use Dozer's SQL equality conversion rules over distinct outer values. Incomparable non-null operands are rejected explicitly. Updates and batches apply their complete inner delta before emitting membership changes.

Marks remain internal to WHERE and are removed before SELECT projection or aggregation, including SELECT *. Multiple and nested subqueries, CTEs, scalar left operands, and compound AND/OR/NOT predicates are supported. Inner CTE names have their own scope. Correlated subqueries are not supported: a reference to an outer table in the inner SELECT is rejected instead of accidentally binding to a same-named inner column. The inner query must return exactly one column. Existing restrictions on ORDER BY and LIMIT also apply.

TTL-bearing inputs are explicitly rejected before mutating either join input. Dozer currently expires join indexes without emitting delete events; supporting TTL membership requires explicit expiration propagation so downstream rows do not become stale. Ordinary insert/update/delete streams and non-TTL window results are supported. Like the existing join processors, state is reconstructed by replaying source events.

Run the SQL-to-DAG regressions with:

```sh
cargo test -p dozer-sql --no-default-features in_subquery
```

These exercise the actual parser, schema validation, generated graph, and processors with deterministic source-port ordering. They cover both arrival orders, duplicate and NULL values, atomic updates and batches, independent compound predicates, nested queries, CTE scope, cross-type equality, shared sources, TTL rejection, and 360 successive stream changes checked against a snapshot oracle.
