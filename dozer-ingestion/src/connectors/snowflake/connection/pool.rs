use std::{
    cell::RefCell,
    collections::LinkedList,
    ops::{Deref, DerefMut},
    rc::Rc,
};

use odbc::{
    odbc_safe::{AutocommitOn, Odbc3},
    Connection, DiagnosticRecord, Environment,
};

use crate::blocking_retry_on_network_failure;

use super::helpers::is_network_failure;

const MAX_POOL_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct Pool<'env> {
    inner: Rc<RefCell<Inner<'env>>>,
}

impl<'env> Pool<'env> {
    pub fn new(env: &'env Environment<Odbc3>, conn_string: String) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                env,
                conn_string,
                connections: Default::default(),
            })),
        }
    }

    pub fn get_conn(&self) -> Result<Conn<'env>, Box<DiagnosticRecord>> {
        let mut inner = self.inner.borrow_mut();
        let inner = inner.deref_mut();
        let conn = if let Some(conn) = inner.connections.pop_front() {
            conn
        } else {
            blocking_retry_on_network_failure!(
                "connect_with_connection_string",
                inner.env.connect_with_connection_string(&inner.conn_string),
                is_network_failure,
            )?
        };
        Ok(Conn {
            pool: self.clone(),
            inner: conn,
        })
    }

    fn return_conn(&self, conn: Connection<'env, AutocommitOn>) {
        let mut inner = self.inner.borrow_mut();
        let inner = inner.deref_mut();
        if inner.connections.len() < MAX_POOL_SIZE {
            inner.connections.push_back(conn);
        }
    }
}

#[derive(Debug)]
struct Inner<'env> {
    env: &'env Environment<Odbc3>,
    conn_string: String,
    connections: LinkedList<Connection<'env, AutocommitOn>>,
}

#[derive(Debug)]
pub struct Conn<'env> {
    pool: Pool<'env>,
    inner: Connection<'env, AutocommitOn>,
}

impl<'env> Conn<'env> {
    /// Returns the connection to the pool.
    /// Currently, connections have to be manually returned to the pool
    /// because odbc::Connection does not know if it has been disconnected.
    pub fn return_(self) {
        self.pool.return_conn(self.inner)
    }
}

impl<'env> Deref for Conn<'env> {
    type Target = Connection<'env, AutocommitOn>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
