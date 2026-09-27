use std::env;
use std::sync::Arc;

use anyhow::Context;
use sqlx::postgres::{PgPoolOptions,PgPool};

mod idxent;
use idxent::server::Server;

// for test of get_site_url_path
use url::Url;
use idxent::server::get_site_url_path;

#[tokio::main]
async fn main() -> anyhow::Result<()>
{
    // test work of get_site_url_path
    let url_site_start_page_str : String = env::var("URL").unwrap_or(String::from(""));
    if url_site_start_page_str.len() != 0
    {
        let url_site_start_page : Url = Url::parse(url_site_start_page_str.as_str())?;
        println!("url_site_start_page: {url_site_start_page}");
        let get_site_url_path : Url = get_site_url_path(&url_site_start_page)?;
        println!("get_site_url_path:   {get_site_url_path}");
        return Ok(());
    }

    let database_url = env::var("DATABASE_URL").context("Env. var. DATABASE_URL is not set")?;
    let db_pool : PgPool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .context(format!("failed connect to {database_url}"))?;

    sqlx::migrate!("./migrations").run(&db_pool).await?;

    let idxent_server = Arc::new(Server::new(&db_pool));

    // run idxent server with hyper, listening locally on port 8080
    let idxent_bind_addr = env::var("IDXENT_BIND_ADDR").unwrap_or(String::from("127.0.0.1:8080"));
    let listener = tokio::net::TcpListener::bind(idxent_bind_addr.clone()).await.unwrap();
    println!("Listening on {}", idxent_bind_addr);
    axum::serve(listener, idxent_server.get_router().clone()).await?;
    Ok(())
}

