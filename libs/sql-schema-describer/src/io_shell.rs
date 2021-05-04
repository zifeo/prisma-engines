use quaint::{prelude::Queryable, single::Quaint, Value};
use std::{borrow::Cow, future::Future, pin::Pin};

#[derive(Debug)]
pub struct DatabaseError {
    pub message: String,
    pub error_code: Option<Cow<'static, str>>,
}

pub type BoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;
pub type DbResult<T> = Result<T, DatabaseError>;

pub trait Row<'a> {
    fn bool_at(&self, idx: usize) -> Option<bool>;
    fn i64_at(&self, idx: usize) -> Option<i64>;
    fn str_at(&self, idx: usize) -> Option<&'a str>;
}

impl<'a> Row<'a> for quaint::connector::ResultRowRef<'a> {
    fn bool_at(&self, idx: usize) -> Option<bool> {
        self.at(idx).and_then(|v| v.as_bool())
    }

    fn i64_at(&self, idx: usize) -> Option<i64> {
        self.at(idx).and_then(|v| v.as_i64())
    }

    fn str_at(&self, idx: usize) -> Option<&'a str> {
        self.at(idx).and_then(|v| v.as_str())
    }
}

pub trait ResultSet {
    fn len(&self) -> usize;
    fn row_at(&'_ self, rowidx: usize) -> Option<Box<dyn Row<'_> + Send + '_>>;
}

impl ResultSet for quaint::connector::ResultSet {
    fn len(&self) -> usize {
        quaint::connector::ResultSet::len(self)
    }

    fn row_at(&self, rowidx: usize) -> Option<Box<dyn Row + Send + '_>> {
        self.get(rowidx).map(|row| -> Box<dyn Row + Send> { Box::new(row) })
    }
}

pub fn iter_rows<'a>(
    rs: &'a (dyn ResultSet + Send + Sync + 'a),
) -> impl Iterator<Item = Box<dyn Row<'a> + Send + 'a>> + 'a {
    struct I<'a> {
        idx: usize,
        inner: &'a (dyn ResultSet + Send + Sync + 'a),
    }

    impl<'a> std::iter::Iterator for I<'a> {
        type Item = Box<dyn Row<'a> + Send + 'a>;

        fn next(&mut self) -> Option<Self::Item> {
            let row = self.inner.row_at(self.idx)?;

            self.idx += 1;

            Some(row)
        }
    }

    let i = I { idx: 0, inner: rs };

    i
}

pub trait IoShell {
    fn query<'a>(
        &'a self,
        query: &'a str,
        params: &'a [&'a str],
    ) -> BoxFuture<'a, DbResult<Box<dyn ResultSet + Send + Sync>>>;
    fn raw_cmd<'a>(&'a self, query: &'a str) -> BoxFuture<'a, DbResult<()>>;
}

impl IoShell for Quaint {
    fn query<'a>(
        &'a self,
        query: &'a str,
        params: &'a [&'a str],
    ) -> BoxFuture<'a, DbResult<Box<dyn ResultSet + Send + Sync>>> {
        let params: Vec<_> = params.iter().map(|s| Value::text(*s)).collect();
        Box::pin(async move {
            <Quaint as Queryable>::query_raw(self, query, &params)
                .await
                .map(|res| -> Box<dyn ResultSet + Send + Sync> { Box::new(dbg!(res)) })
                .map_err(|qerr| DatabaseError {
                    message: qerr.to_string(),
                    error_code: qerr.original_code().map(String::from).map(Cow::from),
                })
        })
    }

    fn raw_cmd<'a>(&'a self, query: &'a str) -> BoxFuture<'a, DbResult<()>> {
        Box::pin(async move {
            <Quaint as Queryable>::raw_cmd(self, query)
                .await
                .map_err(|qerr| DatabaseError {
                    message: qerr.to_string(),
                    error_code: qerr.original_code().map(String::from).map(Cow::from),
                })
        })
    }
}
