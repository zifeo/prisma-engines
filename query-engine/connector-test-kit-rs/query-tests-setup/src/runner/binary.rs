use crate::{ConnectorTag, RunnerInterface, TestResult};
use hyper::{Body, Client, Method, Request};
use query_core::{executor, schema_builder, BuildMode, QueryExecutor, QuerySchemaRef, ResponseData, TxId};
use query_engine::opt::PrismaOpt;
use query_engine::server::{routes, setup, State};
use request_handlers::{GQLResponse, GraphQlBody, GraphQlHandler, MultiQuery, PrismaResponse};
use std::{env, sync::Arc};

pub(crate) type Executor = Box<dyn QueryExecutor + Send + Sync>;

pub struct BinaryRunner {
    opts: PrismaOpt,
    connector_tag: ConnectorTag,
    current_tx_id: Option<TxId>,
}

#[async_trait::async_trait]
impl RunnerInterface for BinaryRunner {
    async fn load(datamodel: String, connector_tag: ConnectorTag) -> TestResult<Self> {
        let mut opts = PrismaOpt::from_list(&[]);
        opts.datamodel = Some(datamodel);

        Ok(BinaryRunner {
            opts,
            connector_tag,
            current_tx_id: None,
        })
    }

    async fn query(&self, query: String) -> TestResult<crate::QueryResult> {
        let state = setup(&self.opts).await.unwrap();

        let op = serde_json::json!({
            "operationName": null,
            "variables": {},
            "query": query
        });

        let body = serde_json::to_vec(&op).unwrap();

        let req = Request::builder().method(Method::POST).body(Body::from(body)).unwrap();

        // let client = Client::new();
        // let resp = client.request(req).await.unwrap();
        let resp = routes(state.clone(), req).await.unwrap();

        println!("RESP {:?}", resp);
        let body_start = resp.into_body();
        println!("BOOM start {:?}", body_start);
        let full_body = hyper::body::to_bytes(body_start).await.unwrap();

        println!(" BODY {:?}", full_body);
        let json_resp: serde_json::Value = serde_json::from_slice(full_body.as_ref()).unwrap();

        Ok(json_resp.into())
    }

    async fn batch(&self, _queries: Vec<String>, _transaction: bool) -> TestResult<crate::QueryResult> {
        todo!()
    }

    fn connector(&self) -> &crate::ConnectorTag {
        &self.connector_tag
    }

    fn executor(&self) -> &dyn QueryExecutor {
        todo!()
        // self.executor.as_ref()
    }

    fn set_active_tx(&mut self, tx_id: query_core::TxId) {
        self.current_tx_id = Some(tx_id);
    }

    fn clear_active_tx(&mut self) {
        self.current_tx_id = None;
    }
}
