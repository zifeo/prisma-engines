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
        // let gql_response: GQLResponse = serde_json::from_slice(full_body.as_ref()).unwrap();
        let json_resp: serde_json::Value = serde_json::from_slice(full_body.as_ref()).unwrap();
        // let data = &json_resp["data"];
        // // let obj = data.as_object().unwrap();

        // // let keys: Vec<_> = obj.keys().collect();
        // // let name = keys[0];

        // // let a = ResponseData::new(name.to_string(), query_core::Item::Json(data.clone()));

        // // let mut a: IndexMap<String, query_core::Item> = IndexMap::with_capacity(0);

        // // obj.iter().for_each(|(k, v)| {
        // //     a.insert(k.to_string(), query_core::Item::Json(v.clone()));
        // // });

        // // // let z = ResponseData::from(_)
        // // println!("HELLO {:?}", data);
        // // // let mut gql_response = GQLResponse::with_capacity(1);
        // // let gql_response = GQLResponse {
        // //     data: a,
        // //     ..Default::default()
        // // };
        // // gql_response.insert_data(name, query_core::Item::Json(serde_json::Value::Object(obj.clone())));
        // let gql_response: GQLResponse = data.clone().into();

        // let p = PrismaResponse::Single(gql_response.into());

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
