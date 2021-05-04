use futures::{
    channel::mpsc::{self, *},
    lock::Mutex,
    SinkExt, StreamExt,
};
use js_sys::Promise;
use sql_schema_describer::io_shell::ResultSet;
use wasm_bindgen::{prelude::*, JsCast};

#[wasm_bindgen]
extern "C" {
    pub type JsIoShell;
    pub type JsResultSet;
    pub type JsResultRow;

    #[wasm_bindgen(structural, method)]
    pub async fn raw_cmd(this: &JsIoShell, query_str: String) -> JsValue;

    /// Should return a ResultRow
    #[wasm_bindgen(structural, method)]
    pub async fn query(this: &JsIoShell, query_str: String, params: Box<[JsValue]>) -> JsValue;

    #[wasm_bindgen(structural, method)]
    pub async fn str_at(this: &JsResultRow) -> JsValue;
}

impl sql_schema_describer::io_shell::IoShell for IoTask {
    fn query<'a>(
        &'a self,
        query: &'a str,
        params: &'a [&'a str],
    ) -> sql_schema_describer::io_shell::BoxFuture<
        'a,
        sql_schema_describer::io_shell::DbResult<Box<dyn sql_schema_describer::io_shell::ResultSet + Send + Sync>>,
    > {
        todo!()
    }

    fn raw_cmd<'a>(
        &'a self,
        query_str: &'a str,
    ) -> sql_schema_describer::io_shell::BoxFuture<'a, sql_schema_describer::io_shell::DbResult<()>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().await;

            guard.0.send((query_str.to_owned(), None)).await.unwrap();
            guard.1.next().await.unwrap();

            Ok(())
        })
    }
}

impl ResultSet for JsResultSet {
    fn len(&self) -> usize {
        todo!()
    }

    fn row_at(&'_ self, rowidx: usize) -> Option<Box<dyn sql_schema_describer::io_shell::Row<'_> + Send + '_>> {
        todo!()
    }
}

struct IoTask {
    inner: Mutex<(
        Sender<(String, Option<Vec<String>>)>,
        Receiver<Option<Box<dyn ResultSet + Send>>>,
    )>,
}

impl IoTask {
    fn spawn(shell: JsIoShell) -> Self {
        let (sender, mut receiver) = mpsc::channel::<(String, Option<Vec<String>>)>(10);
        let (mut response_sender, response_receiver) = mpsc::channel::<Option<Box<dyn ResultSet + Send>>>(10);

        wasm_bindgen_futures::spawn_local(async move {
            while let Some(msg) = receiver.next().await {
                match msg {
                    (query_str, None) => {
                        shell.raw_cmd(query_str).await;
                        response_sender.send(None).await.unwrap();
                    }
                    (query_str, Some(params)) => {
                        let converted_params: Vec<JsValue> = params.iter().map(|p| JsValue::from(p)).collect();
                        let response = shell.query(query_str, converted_params.into_boxed_slice()).await;
                        let js_result_set: JsResultSet = response.dyn_into().unwrap();
                        response_sender.send(Some(Box::new(js_result_set))).await.unwrap();
                    }
                }
            }
        });

        IoTask {
            inner: Mutex::new((sender, response_receiver)),
        }
    }
}

#[wasm_bindgen]
pub async fn describe(io_shell: JsIoShell) -> String {
    let task = IoTask::spawn(io_shell);

    sql_schema_describer::sqlite::SqlSchemaDescriber::new_shell(Box::new(task));

    todo!()
}
