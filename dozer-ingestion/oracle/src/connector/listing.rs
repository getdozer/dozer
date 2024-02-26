use dozer_ingestion_connector::dozer_types::log::debug;
use oracle::Connection;

use super::Error;

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub owner: String,
    pub table_name: String,
    pub column_name: String,
    pub data_type: Option<String>,
    pub nullable: Option<String>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

impl TableColumn {
    pub fn list(connection: &Connection, schemas: &[String]) -> Result<Vec<TableColumn>, Error> {
        assert!(!schemas.is_empty());
        let sql = "
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER IN (SELECT COLUMN_VALUE FROM TABLE(:2))
        ";
        let schemas = super::string_collection(connection, schemas)?;
        debug!("{}, {}", sql, schemas);
        let rows = connection.query_as::<(
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
        )>(sql, &[&schemas])?;

        let mut columns = Vec::new();
        for row in rows {
            let (owner, table_name, column_name, data_type, nullable, precision, scale) = row?;
            let column = TableColumn {
                owner,
                table_name,
                column_name,
                data_type,
                nullable,
                precision,
                scale,
            };
            columns.push(column);
        }
        Ok(columns)
    }
}

#[derive(Debug, Clone)]
pub struct ConstraintColumn {
    pub owner: String,
    pub constraint_name: String,
    pub table_name: String,
    pub column_name: Option<String>,
}

impl ConstraintColumn {
    pub fn list(
        connection: &Connection,
        schemas: &[String],
    ) -> Result<Vec<ConstraintColumn>, Error> {
        assert!(!schemas.is_empty());
        let sql = "
        SELECT
            OWNER,
            CONSTRAINT_NAME,
            TABLE_NAME,
            COLUMN_NAME
        FROM ALL_CONS_COLUMNS
        WHERE OWNER IN (SELECT COLUMN_VALUE FROM TABLE(:2))
        ";
        let schemas = super::string_collection(connection, schemas)?;
        debug!("{}, {}", sql, schemas);
        let rows =
            connection.query_as::<(String, String, String, Option<String>)>(sql, &[&schemas])?;

        let mut columns = Vec::new();
        for row in rows {
            let (owner, constraint_name, table_name, column_name) = row?;
            let column = ConstraintColumn {
                owner,
                constraint_name,
                table_name,
                column_name,
            };
            columns.push(column);
        }
        Ok(columns)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Constraint {
    pub owner: Option<String>,
    pub constraint_name: Option<String>,
}

impl Constraint {
    pub fn list(connection: &Connection, schemas: &[String]) -> Result<Vec<Constraint>, Error> {
        assert!(!schemas.is_empty());
        let sql = "
        SELECT
            OWNER,
            CONSTRAINT_NAME
        FROM ALL_CONSTRAINTS
        WHERE
            OWNER IN (SELECT COLUMN_VALUE FROM TABLE(:2))
            AND
            CONSTRAINT_TYPE = 'P'
        ";
        let schemas = super::string_collection(connection, schemas)?;
        debug!("{}, {}", sql, schemas);
        let rows = connection.query_as::<(Option<String>, Option<String>)>(sql, &[&schemas])?;

        let mut constraints = Vec::new();
        for row in rows {
            let (owner, constraint_name) = row?;
            let constraint = Constraint {
                owner,
                constraint_name,
            };
            constraints.push(constraint);
        }
        Ok(constraints)
    }
}
