use quaint::{prelude::Queryable, single::Quaint, Value};
use std::{borrow::Cow, future::Future, pin::Pin};

#[derive(Debug)]
pub struct DatabaseError {
    pub message: String,
    pub error_code: Option<Cow<'static, str>>,
}

pub type BoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;
pub type DbResult<T> = Result<T, DatabaseError>;

pub trait Row<'a> {
    fn bool_at(&self, idx: usize) -> Option<bool>;
    fn i64_at(&self, idx: usize) -> Option<i64>;
    fn str_at(&self, idx: usize) -> Option<Cow<'a, str>>;
}

impl<'a> Row<'a> for quaint::connector::ResultRowRef<'a> {
    fn bool_at(&self, idx: usize) -> Option<bool> {
        self.at(idx).and_then(|v| v.as_bool())
    }

    fn i64_at(&self, idx: usize) -> Option<i64> {
        self.at(idx).and_then(|v| v.as_i64())
    }

    fn str_at(&self, idx: usize) -> Option<Cow<'a, str>> {
        self.at(idx).and_then(|v| v.as_str()).map(Cow::Borrowed)
    }
}

pub trait ResultSet {
    fn len(&self) -> usize;
    fn row_at(&'_ self, rowidx: usize) -> Option<Box<dyn Row<'_> + '_>>;
}

impl ResultSet for quaint::connector::ResultSet {
    fn len(&self) -> usize {
        quaint::connector::ResultSet::len(self)
    }

    fn row_at(&self, rowidx: usize) -> Option<Box<dyn Row + '_>> {
        self.get(rowidx).map(|row| -> Box<dyn Row> { Box::new(row) })
    }
}

pub fn iter_rows<'a>(rs: &'a (dyn ResultSet + 'a)) -> impl Iterator<Item = Box<dyn Row<'a> + 'a>> + 'a {
    struct I<'a> {
        idx: usize,
        inner: &'a (dyn ResultSet + 'a),
    }

    impl<'a> std::iter::Iterator for I<'a> {
        type Item = Box<dyn Row<'a> + 'a>;

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
    fn query<'a>(&'a self, query: &'a str, params: &'a [&'a str]) -> BoxFuture<'a, DbResult<Box<dyn ResultSet>>>;
    fn raw_cmd<'a>(&'a self, query: &'a str) -> BoxFuture<'a, DbResult<()>>;
}

impl IoShell for Quaint {
    fn query<'a>(&'a self, query: &'a str, params: &'a [&'a str]) -> BoxFuture<'a, DbResult<Box<dyn ResultSet>>> {
        let params: Vec<_> = params.iter().map(|s| Value::text(*s)).collect();
        Box::pin(async move {
            <Quaint as Queryable>::query_raw(self, query, &params)
                .await
                .map(|res| -> Box<dyn ResultSet> { Box::new(dbg!(res)) })
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
