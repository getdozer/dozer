# Streaming `IN` subqueries

`WHERE value IN (SELECT key FROM source)` and `NOT IN` subscribe to both
input streams. An inner insert can emit previously unmatched outer rows;
removing the last matching inner value retracts them. Duplicate inner values
do not multiply the output. Outer duplicates retain their original multiplicity.
Outer updates preserve `UPDATE` when both old and new records match, and produce
`INSERT`/`DELETE` when visibility changes.

The subquery must be uncorrelated and return exactly one column with the same
field type as the outer expression (an untyped outer `NULL` is also allowed).
Multiple membership predicates can be
combined with `AND`, `OR`, `NOT`, and parentheses. Subquery CTEs are local to
that subquery. Output fields, column positions, primary keys, and source
annotations remain those of the outer input.

Membership uses SQL three-valued logic. A matching non-NULL value is true;
otherwise NULL on either side makes membership unknown, except that an empty
inner result makes `IN` false, including for a NULL outer value. `NOT IN`
negates that truth value, and `WHERE` emits only true results.

## Boundaries

- Correlated references and subqueries embedded inside scalar expressions
  such as `CASE` are unsupported and fail during DAG construction.
- Statements combining membership subqueries with `TTL`, `TUMBLE`, or `HOP`
  are rejected during planning. Lifetime-bearing records supplied directly by
  a source are rejected before the operator mutates state. Expiration-aware
  membership is not implemented.
- Right-input changes currently reevaluate retained outer rows. Their cost is
  O(retained outer rows × predicate work), plus copying the affected right-key
  count map for atomic validation. Outer deletion/update searches are linear.
  This is a correctness-first implementation, not an indexed throughput claim.
- All currently retained outer rows and distinct inner keys use memory, as in
  other in-memory stateful operators. No persistence or eviction policy is added.

## Verification

The membership processor tests cover duplicates, retractions, updates, batches,
NULL/empty inputs, boolean composition, multiple subqueries, invalid inputs,
and exact output schema. Explicit expected snapshots were generated separately
with an in-memory SQLite SQL oracle: 18 transitions each for `IN` and `NOT IN`,
plus 13 transitions for a two-subquery boolean condition. Those expectations
are then compared with snapshots reconstructed from actual Dozer output events.
DAG tests check source wiring, CTE scope, unsupported forms, and output columns.

A prerequisite column-resolution fix prevents a qualified reference from
silently binding to the sole same-named column in another table. Paired tests
preserve legitimate unqualified, qualified, aliased, and multi-table resolution.
