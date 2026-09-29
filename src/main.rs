use std::env;
use std::sync::Arc;

use anyhow::Context;
use sqlx::postgres::{PgPoolOptions,PgPool};

mod idxent;

use idxent::config::Config;
use idxent::server::Server;

// extract entity service includes
#[path="extrent/generated/extrent.rs"]
mod extrent;
use extrent::entity_extractor_client::EntityExtractorClient;
type EntityExtractorClientImpl = EntityExtractorClient<tonic::transport::Channel>;


pub async fn connect_to_extrent_service(
    extrent_rcp_addr_str : &String
) -> anyhow::Result<EntityExtractorClientImpl>
{
    // take provided value from config, otherwise from env. variable or default
    let extrent_rcp_addr_str_with_default = 
        if extrent_rcp_addr_str.len() != 0 {extrent_rcp_addr_str}
        else { &env::var("EXTRENT_RCP_ADDR").unwrap_or(String::from("127.0.0.1:8090")) };
    let extrent_rpc_connect_str = format!("http://{}", extrent_rcp_addr_str_with_default);
    
    let context : String = format!("Connecting to EntityExtractorService at {extrent_rpc_connect_str}");
    println!("{context} ... ");
    let client : EntityExtractorClientImpl 
        = EntityExtractorClient::connect(extrent_rpc_connect_str).await
        .context(context.clone())?;

    println!("{context} - done");

    Ok(client)
}


#[tokio::main]
async fn main() -> anyhow::Result<()>
{
    let config : Config = Config::load().unwrap_or_default();
    let extrent_client : EntityExtractorClientImpl  = connect_to_extrent_service(&config.extrent_rcp_addr).await?;

    let database_url = env::var("DATABASE_URL").context("Env. var. DATABASE_URL is not set")?;
    let db_pool : PgPool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .context(format!("failed connect to {database_url}"))?;

    sqlx::migrate!("./migrations").run(&db_pool).await?;

    let idxent_server = Arc::new(Server::new(&config, &db_pool, &extrent_client));

    // run idxent server with hyper, listening locally on port 8080
    let idxent_bind_addr = env::var("IDXENT_BIND_ADDR").unwrap_or(String::from("127.0.0.1:8080"));
    let listener = tokio::net::TcpListener::bind(idxent_bind_addr.clone()).await.unwrap();
    println!("Listening on {}", idxent_bind_addr);
    axum::serve(listener, idxent_server.get_router().clone()).await?;
    Ok(())
}

