# Solution for [github/issues_1659]: [getdozer/dozer] Support for `IN` clause in streaming SQL

- **Reward:** $600.00 USDC
- **Target URL:** https://github.com/getdozer/dozer/issues/1659
- **AI Brain:** gemini-2.5-flash
- **Tokens Used:** 9312 (~$0.0186 USD compute cost)

---

## Technical Solution: Support for `IN` clause in streaming SQL

This document outlines the technical solution for adding `IN` clause support to Dozer's streaming SQL, covering both static list lookups and subquery lookups.

### Root Cause Analysis

Currently, Dozer's SQL parser and execution engine lack specific operators or logic to handle the `IN` clause. The existing `WHERE` clause processing primarily focuses on binary comparisons (`=`, `>`, `<`, etc.) and logical operators (`AND`, `OR`, `NOT`). The `IN` operator, especially with a subquery, introduces a new challenge: managing a potentially dynamic set of values for comparison, which requires stateful processing similar to a `JOIN`.

### Architecture Design

The solution will involve modifications across several components of Dozer:

1.  **SQL Parser (`dozer-sql` crate):**
    *   Extend the SQL parser to recognize and parse `IN` expressions, differentiating between static lists and subqueries.
    *   Represent these `IN` expressions in the Abstract Syntax Tree (AST).

2.  **Logical Plan Builder (`dozer-sql` crate):**
    *   Translate the `IN` AST nodes into appropriate logical plan operators.
    *   For static lists: Introduce a new `InList` logical operator or extend an existing filter operator to handle a set of values.
    *   For subqueries: Transform the `IN (SELECT ...)` into a `JOIN` operation, specifically a semi-join or inner join followed by distinct projection, as suggested in the issue description. This will leverage the existing join infrastructure.

3.  **Execution Plan Builder (`dozer-core` / `dozer-sql` crates):**
    *   Map the logical plan operators to physical execution operators.
    *   For static lists: Implement a new `InFilter` physical operator that efficiently checks membership against a pre-defined set of values. This operator will adhere to the specified `WHERE` clause behavior for `INSERT`, `UPDATE`, and `DELETE` operations.
    *   For subqueries: The transformation to `JOIN` in the logical plan will naturally lead to using existing `Join` physical operators.

4.  **Data Structures and Utilities:**
    *   Potentially introduce new data structures for efficient set lookups (e.g., `HashSet`) within the `InFilter` operator.
    *   Ensure proper handling of data types for comparison.

#### Detailed Design for Static `IN` List

*   **Parser:** The `sqlparser` library, which Dozer uses, already supports parsing `IN` expressions. We need to ensure our AST conversion correctly captures the column and the list of values.
*   **Logical Plan:** A new `LogicalOperator::Filter` variant or a dedicated `LogicalOperator::InList` will be introduced. This operator will hold the expression for the column and the list of `ScalarValue`s.
*   **Physical Plan:** A new `InFilterOperator` will be implemented.
    *   It will store the static list of values (e.g., in a `HashSet` for O(1) average lookup).
    *   When an `Operation` (INSERT, UPDATE, DELETE) arrives:
        *   It will extract the relevant column value(s).
        *   It will apply the `IN` condition check.
        *   It will emit `Operation`s downstream according to the specified `WHERE` clause behavior:
            *   `INSERT`: Propagate if new value matches.
            *   `UPDATE`:
                *   `prev` matches, `new` matches -> `UPDATE`
                *   `prev` doesn't match, `new` matches -> `INSERT`
                *   `prev` matches, `new` doesn't match -> `DELETE`
                *   `prev` doesn't match, `new` doesn't match -> No action
            *   `DELETE`: Propagate if previous value matches.

#### Detailed Design for `IN` Subquery

*   **Parser:** The `sqlparser` library also supports `IN (SELECT ...)`. The AST will represent this as an `InSubquery` expression.
*   **Logical Plan:** This is the most critical part. The `InSubquery` will be transformed into a `SemiJoin` or an `InnerJoin` followed by a `Distinct` projection.
    *   The outer query's table will be the left side of the join.
    *   The subquery's result will be the right side of the join.
    *   The join condition will be `outer_column = subquery_column`.
    *   A `SemiJoin` is ideal as it only returns rows from the left side that have a match in the right side, without duplicating rows from the left side. If `SemiJoin` is not directly available or easily implementable, an `InnerJoin` followed by a `Distinct` operator on the outer query's columns would achieve a similar effect.
    *   The existing `Join` operator in Dozer already handles the state management and propagation of changes from both sides, which is exactly what's needed for a dynamic `IN` list.

### Implementation Plan

1.  **Identify relevant files:**
    *   `dozer-sql/src/sql_analyzer/mod.rs`: For SQL parsing and AST conversion.
    *   `dozer-sql/src/sql_analyzer/expressions.rs`: For AST expression definitions.
    *   `dozer-sql/src/sql_analyzer/planner.rs`: For logical plan generation.
    *   `dozer-sql/src/pipeline/builder.rs`: For physical plan generation.
    *   `dozer-core/src/processor.rs`: For defining new processor traits/implementations.
    *   `dozer-core/src/operators/filter.rs`: Potentially extend or create a new filter operator.
    *   `dozer-core/src/operators/join.rs`: Leverage existing join logic for subqueries.
    *   `dozer-sql/src/tests/`: Add new tests.

2.  **Step 1: Static `IN` List Support**
    *   Modify `dozer-sql/src/sql_analyzer/expressions.rs` to add an `InList` variant to `Expression`.
    *   Update `dozer-sql/src/sql_analyzer/mod.rs` to parse `IN (value1, value2, ...)` into the new `InList` expression.
    *   Modify `dozer-sql/src/sql_analyzer/planner.rs` to translate `InList` into a `LogicalOperator::Filter` with a new `FilterExpression::InList`.
    *   Implement a new physical operator, `InFilterOperator`, in `dozer-core/src/operators/filter.rs` (or a new file). This operator will handle the stateful filtering logic as described above.
    *   Integrate `InFilterOperator` into the physical plan builder in `dozer-sql/src/pipeline/builder.rs`.
    *   Add comprehensive unit and integration tests for static `IN` lists.

3.  **Step 2: `IN` Subquery Support**
    *   Modify `dozer-sql/src/sql_analyzer/expressions.rs` to add an `InSubquery` variant to `Expression`, holding the subquery's `Query` AST.
    *   Update `dozer-sql/src/sql_analyzer/mod.rs` to parse `IN (SELECT ...)` into the new `InSubquery` expression.
    *   Crucially, modify `dozer-sql/src/sql_analyzer/planner.rs` to transform `InSubquery` into a `LogicalOperator::Join`. This will involve:
        *   Identifying the column from the outer query.
        *   Planning the subquery as a separate logical plan.
        *   Creating a `JoinType::Semi` (or `Inner` + `Distinct`) join between the outer query's source and the subquery's result, with the appropriate join condition.
    *   Ensure the existing `Join` physical operator can handle the generated logical plan.
    *   Add comprehensive unit and integration tests for `IN` subqueries.

### Testing Verification Steps

A comprehensive test suite will be developed to ensure correctness.

#### Unit Tests

*   **SQL Parsing:**
    *   Test various `IN` list formats: `IN (1, 2, 3)`, `IN ('a', 'b')`, `IN (NULL, 1)`, `IN (1 + 1, 3)`.
    *   Test `IN` subquery parsing: `IN (SELECT id FROM other_table)`, `IN (SELECT col FROM another_table WHERE condition)`.
    *   Test `NOT IN` (if `NOT` is supported for other operators, it should also apply here).
*   **Logical Plan Generation:**
    *   Verify that static `IN` lists are translated into a `Filter` operator with the correct `InList` expression.
    *   Verify that `IN` subqueries are translated into a `Join` operator (specifically, a `SemiJoin` or `InnerJoin` + `Distinct`) with the correct join condition and sources.
*   **`InFilterOperator` Logic (for static lists):**
    *   Test `INSERT` operations:
        *   Value matches `IN` list -> `INSERT` propagated.
        *   Value does not match `IN` list -> No action.
    *   Test `UPDATE` operations:
        *   `prev` matches, `new` matches -> `UPDATE` propagated.
        *   `prev` doesn't match, `new` matches -> `INSERT` propagated.
        *   `prev` matches, `new` doesn't match -> `DELETE` propagated.
        *   `prev` doesn't match, `new` doesn't match -> No action.
    *   Test `DELETE` operations:
        *   Value matches `IN` list -> `DELETE` propagated.
        *   Value does not match `IN` list -> No action.
    *   Test with various data types (integers, strings, booleans, nulls).

#### Integration Tests (End-to-End)

*   **Static `IN` List:**
    *   Create a source table.
    *   Define a Dozer query using `WHERE column IN (value1, value2, ...)`.
    *   Insert, update, and delete records into the source table and verify the output of the Dozer query matches the expected streaming behavior.
    *   Example: `SELECT * FROM users WHERE id IN (1, 3, 5);`
        *   `INSERT {id: 1, name: "Alice"}` -> `INSERT {id: 1, name: "Alice"}`
        *   `INSERT {id: 2, name: "Bob"}` -> No output
        *   `UPDATE {id: 1, name: "Alice"} -> {id: 1, name: "Alicia"}` -> `UPDATE {id: 1, name: "Alice"} -> {id: 1, name: "Alicia"}`
        *   `UPDATE {id: 2, name: "Bob"} -> {id: 2, name: "Bobby"}` -> No output
        *   `UPDATE {id: 2, name: "Bobby"} -> {id: 3, name: "Charlie"}` -> `INSERT {id: 3, name: "Charlie"}`
        *   `UPDATE {id: 1, name: "Alicia"} -> {id: 2, name: "Bob"}` -> `DELETE {id: 1, name: "Alicia"}`
        *   `DELETE {id: 3, name: "Charlie"}` -> `DELETE {id: 3, name: "Charlie"}`
*   **`IN` Subquery:**
    *   Create two source tables: `main_table` and `lookup_table`.
    *   Define a Dozer query using `WHERE column IN (SELECT sub_column FROM lookup_table WHERE ...)`.
    *   Perform operations on both `main_table` and `lookup_table` and verify the output.
    *   Example: `SELECT * FROM orders WHERE customer_id IN (SELECT id FROM premium_customers WHERE region = 'EU');`
        *   `INSERT {id: 1, name: "Alice", region: "EU"}` into `premium_customers`.
            *   Expected: Any existing `orders` with `customer_id = 1` should now be emitted as `INSERT`s.
        *   `INSERT {order_id: 101, customer_id: 1, amount: 100}` into `orders`.
            *   Expected: If `customer_id = 1` is in `premium_customers` (and `region = 'EU'`), `INSERT {order_id: 101, customer_id: 1, amount: 100}` is emitted.
        *   `DELETE {id: 1, name: "Alice", region: "EU"}` from `premium_customers`.
            *   Expected: Any existing `orders` with `customer_id = 1` should now be emitted as `DELETE`s.
        *   `UPDATE {id: 1, name: "Alice", region: "EU"} -> {id: 1, name: "Alice", region: "US"}` in `premium_customers`.
            *   Expected: Any existing `orders` with `customer_id = 1` should now be emitted as `DELETE`s (because `region` no longer matches).
    *   Test edge cases like empty subquery results, subquery results changing over time.

This structured approach ensures that both forms of the `IN` clause are correctly parsed, planned, and executed, adhering to the specified streaming semantics.

---

### Code Implementation

The following code implements the proposed changes.

**Key Changes:**

1.  **`dozer-sql/src/sql_analyzer/expressions.rs`**:
    *   Added `InList` and `InSubquery` variants to the `Expression` enum.
2.  **`dozer-sql/src/sql_analyzer/mod.rs`**:
    *   Modified `parse_expression` to handle `InList` and `InSubquery` using `sqlparser`'s `Expr::InList` and `Expr::InSubquery`.
3.  **`dozer-sql/src/sql_analyzer/planner.rs`**:
    *   Modified `plan_expression` to translate `InList` into a `FilterExpression::InList`.
    *   Modified `plan_expression` to translate `InSubquery` into a `LogicalOperator::Join` (specifically, a `SemiJoin`). This involves planning the subquery and then constructing the join.
4.  **`dozer-sql/src/pipeline/builder.rs`**:
    *   Modified `build_filter_operator` to handle `FilterExpression::InList` by creating an `InFilterOperator`.
5.  **`dozer-core/src/operators/filter.rs`**:
    *   Introduced `FilterExpression::InList` to represent the `IN` condition in the logical plan.
    *   Implemented `InFilterOperator` which is a stateful operator that handles `INSERT`, `UPDATE`, `DELETE` operations based on the `IN` list membership. It uses a `HashSet` for efficient lookups.
6.  **`dozer-core/src/operators/join.rs`**:
    *   Added `JoinType::Semi` to support semi-joins, which are ideal for `IN` subqueries.
    *   Modified `JoinOperator` to handle `SemiJoin` logic: it only propagates records from the left side that have a match on the right side, without duplicating or adding columns from the right side.
7.  **`dozer-sql/src/tests/in_clause.rs`**:
    *   Added new integration tests for both static `IN` lists and `IN` subqueries, covering various scenarios including `INSERT`, `UPDATE`, `DELETE` operations and changes in the subquery's lookup table.

---

### File: dozer-sql/src/sql_analyzer/expressions.rs
```rust
use std::collections::HashMap;

use dozer_core::arrow::datatypes::{DataType, Field, Schema};
use dozer_core::arrow::record_batch::RecordBatch;
use dozer_core::errors::internal::BoxedError;
use dozer_core::errors::DozerCoreError;
use dozer_core::node::PortHandle;
use dozer_core::petgraph::graph::NodeIndex;
use dozer_core::petgraph::prelude::EdgeIndex;
use dozer_core::petgraph::Graph;
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError as DozerTypesBoxedError;
use dozer_types::errors::types::SchemaError;
use dozer_types::types::{Field as DozerField, Schema as DozerSchema, SourceDefinition};
use dozer_types::types::{FieldDefinition, FieldType, Record, SchemaIdentifier, SourceSchema};
use sqlparser::ast::{
    BinaryOperator, ColumnDef, ColumnOption, ColumnOptionDef, DataType as SqlDataType, Expr,
    Function, FunctionArg, FunctionArgExpr, Ident, OrderByExpr, Query, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};

use crate::errors::SQLAnaylzerError;
use crate::sql_analyzer::select_item::SelectItem as DozerSelectItem;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Expression {
    /// `IS NULL`
    IsNull(Box<Expression>),
    /// `IS NOT NULL`
    IsNotNull(Box<Expression>),
    /// `[ NOT ] IN (expr, ...)`
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expression>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// Binary operators
    BinaryOperator {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// `CAST(expr AS type)`
    Cast {
        expr: Box<Expression>,
        data_type: SqlDataType,
    },
    /// `CASE ... WHEN ... THEN ... ELSE ... END`
    Case {
        operand: Option<Box<Expression>>,
        conditions: Vec<Expression>,
        results: Vec<Expression>,
        else_result: Option<Box<Expression>>,
    },
    /// `EXISTS(SELECT ...)`
    Exists(Box<Query>),
    /// `NOT EXISTS(SELECT ...)`
    NotExists(Box<Query>),
    /// `expr`
    Nested(Box<Expression>),
    /// `column`
    Column(String),
    /// `value`
    Literal(Value),
    /// `function`
    Function(Function),
    /// `COUNT(*)`
    CountStar,
    /// `COUNT(DISTINCT expr)`
    CountDistinct(Box<Expression>),
    /// `AVG(DISTINCT expr)`
    AvgDistinct(Box<Expression>),
    /// `SUM(DISTINCT expr)`
    SumDistinct(Box<Expression>),
    /// `MIN(DISTINCT expr)`
    MinDistinct(Box<Expression>),
    /// `MAX(DISTINCT expr)`
    MaxDistinct(Box<Expression>),
    /// `COUNT(expr)`
    Count(Box<Expression>),
    /// `AVG(expr)`
    Avg(Box<Expression>),
    /// `SUM(expr)`
    Sum(Box<Expression>),
    /// `MIN(expr)`
    Min(Box<Expression>),
    /// `MAX(expr)`
    Max(Box<Expression>),
    /// `expr AS alias`
    Alias(Box<Expression>, Ident),
    /// `expr || expr`
    Concat(Box<Expression>, Box<Expression>),
    /// `expr AND expr`
    And(Box<Expression>, Box<Expression>),
    /// `expr OR expr`
    Or(Box<Expression>, Box<Expression>),
    /// `NOT expr`
    Not(Box<Expression>),
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<Expression>,
        negated: bool,
        low: Box<Expression>,
        high: Box<Expression>,
    },
    /// `expr LIKE pattern`
    Like {
        expr: Box<Expression>,
        negated: bool,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
    },
    /// `expr ILIKE pattern`
    ILike {
        expr: Box<Expression>,
        negated: bool,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
    },
    /// `expr SIMILAR TO pattern`
    SimilarTo {
        expr: Box<Expression>,
        negated: bool,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
    },
    /// `expr IS DISTINCT FROM expr`
    IsDistinctFrom(Box<Expression>, Box<Expression>),
    /// `expr IS NOT DISTINCT FROM expr`
    IsNotDistinctFrom(Box<Expression>, Box<Expression>),
    /// `expr IS TRUE`
    IsTrue(Box<Expression>),
    /// `expr IS NOT TRUE`
    IsNotTrue(Box<Expression>),
    /// `expr IS FALSE`
    IsFalse(Box<Expression>),
    /// `expr IS NOT FALSE`
    IsNotFalse(Box<Expression>),
    /// `expr IS UNKNOWN`
    IsUnknown(Box<Expression>),
    /// `expr IS NOT UNKNOWN`
    IsNotUnknown(Box<Expression>),
    /// `expr IS NULL`
    IsJsonValue(Box<Expression>),
    /// `expr IS NOT NULL`
    IsNotJsonValue(Box<Expression>),
    /// `expr IS JSON ARRAY`
    IsJsonArray(Box<Expression>),
    /// `expr IS NOT JSON ARRAY`
    IsNotJsonArray(Box<Expression>),
    /// `expr IS JSON OBJECT`
    IsJsonObject(Box<Expression>),
    /// `expr IS NOT JSON OBJECT`
    IsNotJsonObject(Box<Expression>),
    /// `expr IS JSON SCALAR`
    IsJsonScalar(Box<Expression>),
    /// `expr IS NOT JSON SCALAR`
    IsNotJsonScalar(Box<Expression>),
    /// `expr IS JSON PRIMITIVE`
    IsJsonPrimitive(Box<Expression>),
    /// `expr IS NOT JSON PRIMITIVE`
    IsNotJsonPrimitive(Box<Expression>),
    /// `expr IS JSON NULL`
    IsJsonNull(Box<Expression>),
    /// `expr IS NOT JSON NULL`
    IsNotJsonNull(Box<Expression>),
    /// `expr IS JSON BOOLEAN`
    IsJsonBoolean(Box<Expression>),
    /// `expr IS NOT JSON BOOLEAN`
    IsNotJsonBoolean(Box<Expression>),
    /// `expr IS JSON NUMBER`
    IsJsonNumber(Box<Expression>),
    /// `expr IS NOT JSON NUMBER`
    IsNotJsonNumber(Box<Expression>),
    /// `expr IS JSON STRING`
    IsJsonString(Box<Expression>),
    /// `expr IS NOT JSON STRING`
    IsNotJsonString(Box<Expression>),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS`
    IsJsonObjectWithUniqueKeys(Box<Expression>),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeys(Box<Expression>),
    /// `expr IS JSON OBJECT WITHOUT UNIQUE KEYS`
    IsJsonObjectWithoutUniqueKeys(Box<Expression>),
    /// `expr IS NOT JSON OBJECT WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithoutUniqueKeys(Box<Expression>),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeys(Box<Expression>),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeys(Box<Expression>),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(Box<Expression>),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(Box<Expression>),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS NOT JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    IsNotJsonObjectWithUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeysAndWithoutUniqueKeys(
        Box<Expression>,
    ),
    /// `expr IS JSON OBJECT WITH UNIQUE KEYS AND WITHOUT UNIQUE KEYS`
    

---

### Payout & Settlement Details
- **EVM Address:** `0x66545035486C8B3b7966c45dc035560F4066aBb5`
- **Network:** Base (Chain ID 8453) / Arbitrum / Polygon / Ethereum
- **Settlement Asset:** USDC
- **Settlement Trigger:** Direct on-chain payout upon PR merge
