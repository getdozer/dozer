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

## Evaluation and scope

Each retained outer occurrence also records whether it was actually emitted.
An outer insert/batch evaluates its new rows; an update evaluates the new row;
a delete retracts the stored occurrence without reevaluating its old predicate.
A right-input event reevaluates retained rows against the complete new right
multiset and compares with their stored visibility. Thus volatile expressions
such as `NOW()` cannot erase the history needed to retract an emitted row.
They are event-driven: time passing alone does not schedule reevaluation.
Even a duplicate right-key event may change a volatile predicate's result.

Nested WITH lists may shadow enclosing CTE names, while duplicate names within
one WITH list remain invalid. Local bindings are restored on return. Newly
planned CTE branches that cannot reach an output are omitted, preserving any
pre-existing pipeline processors and sinks. Dynamic source schemas obtain the
qualifier declared by FROM; existing table/connection metadata and explicit
aliases remain authoritative. Named CTE and derived-query results establish a
new relation scope: their source annotations are rebound to the declared name
before reuse, for both wildcard and explicit projections. Column layout, types,
nullability, and keys are preserved; underlying source qualifiers do not leak
through that boundary.

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
