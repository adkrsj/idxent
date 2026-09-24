mod idxent;
use idxent::server::Server;

use std::error::Error;
use std::env;
use std::sync::Arc;

// for test
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

    let idxent_server = Arc::new(Server::new());

    // run idxent server with hyper, listening locally on port 8080
    let idxent_bind_addr = env::var("IDXENT_BIND_ADDR").unwrap_or(String::from("127.0.0.1:8080"));
    let listener = tokio::net::TcpListener::bind(idxent_bind_addr.clone()).await.unwrap();
    println!("Listening on {}", idxent_bind_addr);
    axum::serve(listener, idxent_server.get_router().clone()).await?;
    Ok(())
}

