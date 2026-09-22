@@
-use anyhow::Result;
-use sqlparser::ast::Expr;
+use anyhow::Result;
+use sqlparser::ast::Expr;
+use crate::pipeline::record::Record;
+use crate::pipeline::in_clause::evaluate_in;
@@
-                match expr {
-                    // existing match arms
-                }
+                match expr {
+                    // existing match arms
+
+                    // -----------------------------------------------------------------
+                    // Added support for the IN clause with a static list of literals.
+                    // Example:  WHERE column_name IN (value1, value2, ...)
+                    // -----------------------------------------------------------------
+                    Expr::InList { expr, list, negated } => {
+                        // Extract the column name from the left‑hand side expression.
+                        let column = match expr.as_ref() {
+                            Expr::Identifier(ident) => ident.value.clone(),
+                            Expr::CompoundIdentifier(idents) => {
+                                idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
+                            }
+                            _ => {
+                                return Err(anyhow::anyhow!(
+                                    "IN clause left side must be a column identifier"
+                                ));
+                            }
+                        };
+
+                        // Convert the list of literals into a vector of strings.
+                        // Currently we only support static literal values (no sub‑queries).
+                        let mut static_values = Vec::new();
+                        for v in list {
+                            match v {
+                                Expr::Value(lit) => static_values.push(lit.to_string()),
+                                _ => {
+                                    return Err(anyhow::anyhow!(
+                                        "IN clause only supports static literal values"
+                                    ));
+                                }
+                            }
+                        }
+
+                        // Build a closure that evaluates the IN condition for a record.
+                        // The closure returns `true` when the record's column value is
+                        // present in the static list (or not present when the clause is
+                        // negated, i.e. `NOT IN`).
+                        let filter = move |record: &Record| {
+                            // The `Record` type in Dozer is a map‑like structure that
+                            // provides a `get` method returning a `Value`.  For the
+                            // purpose of the IN check we compare the string
+                            // representation of the value.
+                            let field_val = record.get(&column);
+                            let contains = static_values.iter().any(|val| {
+                                // Simple string comparison – sufficient for the
+                                // supported literal types (numbers, strings, etc.).
+                                val == &field_val.to_string()
+                            });
+                            if *negated { !contains } else { contains }
+                        };
+
+                        Ok(Box::new(filter))
+                    }
+                }
