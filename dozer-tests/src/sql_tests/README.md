## Overview
This is Dozer's [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) implementation. It uses [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to parse test files and run test cases.

### Basic usage
Run all tests
```shell
cargo test --test sql-tests
```

Auto complete test file which is very convenient. It will use the validator database (currently sqlite) to produce the expected output for the test file.
```shell
cargo test --test sql-tests -- --complete
```

### Sqllogictest
Most records are either a statement or a query. A statement is an SQL command that is to be evaluated but from which we do not expect to get results (other than success or failure). A statement might be a CREATE TABLE or an INSERT or an UPDATE or a DROP INDEX. A query is an SQL command from which we expect to receive results. The result set might be empty.

A statement record begins with one of the following two lines:
```
statement ok
statement error <error info>
```
The SQL command to be evaluated is found on the second and all subsequent liens of the record. Only a single SQL command is allowed per statement. The SQL should not have a semicolon or other terminator at the end.

A query record begins with a line of the following form:
```
# comments
query <type_string> <sort_mode> <label>
<sql_query>
----
<expected_result>
```
The SQL for the query is found on second an subsequent lines of the record up to first line of the form "----" or until the end of the record. Lines following the "----" are expected results of the query, one value per line. If the "----" and/or the results are omitted, then the query is expected to return an empty set.

For more information about arguments, such as <type_string>, <sort_mode>, <label> please refer to [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).
