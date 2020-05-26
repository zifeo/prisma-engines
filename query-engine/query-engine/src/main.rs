#[macro_use]
extern crate tracing;
#[macro_use]
extern crate rust_embed;

use crate::context::PrismaContext;
use crate::request_handlers::{GraphQlBody, GraphQlRequestHandler};
use cli::*;
use error::*;
use futures::TryFutureExt;
use once_cell::sync::Lazy;
use opt::*;
use request_handlers::{PrismaRequest, PrismaResponse, RequestHandler};
use server::{HttpServer, HttpServerBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use std::{convert::TryFrom, error::Error, net::SocketAddr, process};
use structopt::StructOpt;
use tracing::subscriber;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod cli;
mod context;
mod dmmf;
mod error;
mod exec_loader;
mod opt;
mod request_handlers;
mod server;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum LogFormat {
    Text,
    Json,
}

static LOG_FORMAT: Lazy<LogFormat> =
    Lazy::new(|| match std::env::var("RUST_LOG_FORMAT").as_ref().map(|s| s.as_str()) {
        Ok("devel") => LogFormat::Text,
        _ => LogFormat::Json,
    });

pub type PrismaResult<T> = Result<T, PrismaError>;
type AnyError = Box<dyn Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    init_logger()?;
    let opts = PrismaOpt::from_args();

    match CliCommand::try_from(&opts) {
        Ok(cmd) => {
            if let Err(err) = cmd.execute().await {
                info!("Encountered error during initialization:");
                err.render_as_json().expect("error rendering");
                process::exit(1);
            }
        }
        Err(PrismaError::InvocationError(_)) => {
            set_panic_hook()?;
            start_server(opts).await
        }
        Err(err) => {
            info!("Encountered error during initialization:");
            err.render_as_json().expect("error rendering");
            process::exit(1);
        }
    }

    Ok(())
}

fn init_logger() -> Result<(), AnyError> {
    match *LOG_FORMAT {
        LogFormat::Text => {
            let subscriber = FmtSubscriber::builder()
                .with_env_filter(EnvFilter::from_default_env())
                .finish();

            subscriber::set_global_default(subscriber)?;
        }
        LogFormat::Json => {
            let subscriber = FmtSubscriber::builder()
                .json()
                .with_env_filter(EnvFilter::from_default_env())
                .finish();

            subscriber::set_global_default(subscriber)?;
        }
    }

    Ok(())
}

fn set_panic_hook() -> Result<(), AnyError> {
    match *LOG_FORMAT {
        LogFormat::Text => (),
        LogFormat::Json => {
            std::panic::set_hook(Box::new(|info| {
                let payload = info
                    .payload()
                    .downcast_ref::<String>()
                    .map(Clone::clone)
                    .unwrap_or_else(|| info.payload().downcast_ref::<&str>().unwrap().to_string());

                match info.location() {
                    Some(location) => {
                        tracing::event!(
                            tracing::Level::ERROR,
                            message = "PANIC",
                            reason = payload.as_str(),
                            file = location.file(),
                            line = location.line(),
                            column = location.column(),
                        );
                    }
                    None => {
                        tracing::event!(tracing::Level::ERROR, message = "PANIC", reason = payload.as_str());
                    }
                }

                std::process::exit(255);
            }));
        }
    }

    Ok(())
}

async fn start_server(opts: PrismaOpt) {
    // let server_impl = "http-tcp";
    let server_impl = "json-rpc-ws";
    match server_impl {
        "http-tcp" => start_http_server(opts).await,
        "json-rpc-ws" => start_json_rpc_ws_server(opts).await,
        _ => panic!("Unknown server impl {}", server_impl),
    }
}

async fn start_http_server(opts: PrismaOpt) {
    let ip = opts.host.parse().expect("Host was not a valid IP address");
    let address = SocketAddr::new(ip, opts.port);

    eprintln!("Printing to stderr for debugging");
    eprintln!("Listening on {}:{}", opts.host, opts.port);

    let create_builder = move || {
        let config = opts.configuration(false)?;
        let datamodel = opts.datamodel(false)?;

        PrismaResult::<HttpServerBuilder>::Ok(
            HttpServer::builder(config, datamodel)
                .legacy(opts.legacy)
                .enable_raw_queries(opts.enable_raw_queries)
                .enable_playground(opts.enable_playground),
        )
    };

    let builder = match create_builder() {
        Err(err) => {
            info!("Encountered error during initialization:");
            err.render_as_json().expect("error rendering");
            process::exit(1);
        }
        Ok(builder) => builder,
    };

    if let Err(err) = builder.build_and_run(address).await {
        info!("Encountered error during initialization:");
        err.render_as_json().expect("error rendering");
        process::exit(1);
    };
}

async fn start_json_rpc_ws_server(opts: PrismaOpt) {
    use futures::FutureExt;
    use jsonrpc_core::Params;
    use jsonrpc_ws_server::jsonrpc_core::IoHandler;
    use jsonrpc_ws_server::*;
    use serde_json::Value;

    let config = opts.configuration(false).unwrap();
    let datamodel = opts.datamodel(false).unwrap();
    let ctx = PrismaContext::builder(config, datamodel).build().await.unwrap();
    let arcified_ctx = Arc::new(ctx);

    let mut io = IoHandler::new();
    io.add_method("say_hello", |_params| Ok(Value::String("hello".into())));
    io.add_method("query", move |params: Params| {
        let cloned = arcified_ctx.clone();
        let fut = async move {
            handle_rpc_call(params, &cloned)
                .await
                .map_err(|_| jsonrpc_core::Error::internal_error())
        };
        fut.boxed().compat()
    });

    let server = ServerBuilder::new(io)
        .start(&"0.0.0.0:3030".parse().unwrap())
        .expect("Server must start with no issues");

    server.wait().unwrap()
}

async fn handle_rpc_call(
    params: jsonrpc_core::Params,
    ctx: &Arc<PrismaContext>,
) -> Result<serde_json::Value, anyhow::Error> {
    let body: GraphQlBody = params.clone().parse().unwrap();
    let req = PrismaRequest {
        body,
        path: "".to_string(),
        headers: HashMap::new(),
    };

    let result = GraphQlRequestHandler.handle(req, &ctx).await;
    let json = serde_json::to_value(&result).unwrap();

    Ok(json)
}
