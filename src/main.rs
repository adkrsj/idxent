mod idxent;
use idxent::server::Server;

use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main()
{
    let idxent_server = Arc::new(Server::new());

    // run idxent server with hyper, listening locally on port 8080
    let idxent_bind_addr = env::var("IDXENT_BIND_ADDR").unwrap_or(String::from("127.0.0.1:8080"));
    let listener = tokio::net::TcpListener::bind(idxent_bind_addr.clone()).await.unwrap();
    println!("Listening on {}", idxent_bind_addr);
    axum::serve(listener, idxent_server.get_router().clone()).await.unwrap();
}

