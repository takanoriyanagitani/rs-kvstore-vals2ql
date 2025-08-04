use std::io;

use std::process::ExitCode;

use tokio::net::TcpListener;

use rs_kvstore_vals2ql::async_graphql;
use rs_kvstore_vals2ql::deadpool_redis;

use deadpool_redis::Pool;

use async_graphql_axum::GraphQLRequest;
use async_graphql_axum::GraphQLResponse;

use async_graphql::Request;
use async_graphql::dataloader::DataLoader;

use rs_kvstore_vals2ql::KvsSchema;
use rs_kvstore_vals2ql::RedisLike;
use rs_kvstore_vals2ql::StringLoader;
use rs_kvstore_vals2ql::url2pool;
use rs_kvstore_vals2ql::url2schema;

async fn req2res_s(s: &KvsSchema, req: GraphQLRequest, p: &Pool) -> GraphQLResponse {
    let q: Request = req.into_inner();
    let rl = RedisLike { pool: p.clone() };
    let sl = StringLoader { conn: rl };
    let q = q.data(DataLoader::new(sl, tokio::spawn).max_batch_size(2));
    s.execute(q).await.into()
}

fn env2redis_url() -> Result<String, io::Error> {
    match std::env::var("REDIS_URL") {
        Ok(url) => Ok(url),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Failed to get REDIS_URL: {e}"),
        )),
    }
}

fn env2addr_port() -> Result<String, io::Error> {
    match std::env::var("ADDR_PORT") {
        Ok(port) => Ok(port),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Failed to get ADDR_PORT: {e}"),
        )),
    }
}

async fn sub() -> Result<(), io::Error> {
    let url: String = env2redis_url()?;
    let aport: String = env2addr_port()?;

    let pool = url2pool(&url)?;

    let sch = url2schema(&url)?;
    let sdl: String = sch.sdl();
    std::fs::write("./redis2vals2ql.gql", sdl.as_bytes())?;

    let lis = TcpListener::bind(aport).await?;

    let app = axum::Router::new().route(
        "/",
        axum::routing::post(|req| async move { req2res_s(&sch, req, &pool).await }),
    );

    axum::serve(lis, app).await
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
