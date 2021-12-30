use crate::{ConnectorTag, RunnerInterface, TestResult};
use prisma_models::InternalDataModelBuilder;
use query_core::{executor, schema_builder, BuildMode, QueryExecutor, QuerySchemaRef, ResponseData, TxId};
// use query_engine::server::*;
use hyper::{Body, Client, Method, Request};
use request_handlers::{GQLResponse, GraphQlBody, GraphQlHandler, MultiQuery, PrismaResponse};
use std::{env, sync::Arc};

pub(crate) type Executor = Box<dyn QueryExecutor + Send + Sync>;

pub struct BinaryRunner {
    // executor: Executor,
    // query_schema: QuerySchemaRef,
    connector_tag: ConnectorTag,
    current_tx_id: Option<TxId>,
}

#[async_trait::async_trait]
impl RunnerInterface for BinaryRunner {
    async fn load(datamodel: String, connector_tag: ConnectorTag) -> TestResult<Self> {
        todo!()
    }

    async fn query(&self, query: String) -> TestResult<crate::QueryResult> {
        let req = Request::builder()
            .method(Method::POST)
            .uri("http://localhost:4466/")
            .body(Body::from(query))
            .unwrap();

        let client = Client::new();
        let resp = client.request(req).await.unwrap();

        let body_start = resp.into_body();
        let full_body = hyper::body::to_bytes(body_start).await.unwrap();

        let json_resp: serde_json::Value = serde_json::from_slice(full_body.as_ref()).unwrap();

        // let z = ResponseData::from(_)
        let gql_response = GQLResponse::with_capacity(1);
        gql_response.insert_data("data", query_core::Item::Json(json_resp["data"]));

        let p = PrismaResponse::Single(gql_response);
        // // let p: PrismaResponse = serde_json::json!("boom").into();

        Ok(p.into())
    }

    async fn batch(&self, queries: Vec<String>, transaction: bool) -> TestResult<crate::QueryResult> {
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
