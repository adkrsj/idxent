use std::env;
use std::fs;

use anyhow::Context;

use reqwest;
use deformat::{extract,Extracted,Error};

use tonic::Request;

#[path="generated/extrent.rs"]
mod extrent;

use extrent::entity_extractor_client::EntityExtractorClient;
use extrent::{ExtractEntityRequest};


#[tokio::main]
async fn main() -> anyhow::Result<()> 
{
    // read text from url or from file, specified in env. vars
    let mut text : String = String::from("");

    if let Ok(source_url) = env::var("SOURCE_URL")
    {
        println!("SOURCE_URL: {}", source_url);

        let html = reqwest::ClientBuilder::new()
            .user_agent(env::var("USER_AGENT").unwrap_or("Mozilla/5.0".to_string()))
            .build()? 
            .get(source_url.as_str()) // obtain RequestBuilder
            .send() // construct the Request and send it to the target URL, returning a future Response
            .await?
            .error_for_status()? // turn HTTP errors into Rust errors
            .text() // read response body as text
            .await?;

        text = deformat::extract(&html).context("extract text").unwrap().text;
    }
    else if let Ok(source_file) = env::var("SOURCE_FILE")
    {
        println!("SOURCE_FILE: {}", source_file);
        text = fs::read_to_string(&source_file).unwrap_or_else(
            |error|{ panic!("Failed reading from file {}: {}", source_file, error) } );
    }

    println!("\nSOURCE TEXT:\n{}\n", text);

    let mut entity_kinds : Vec<String> = vec![];
    let entity_kinds_var = env::var("ENTITY_KINDS");
    if entity_kinds_var.is_ok()
    {
        let entity_kinds_str : String = entity_kinds_var.unwrap();
        entity_kinds = entity_kinds_str.split(",").map(String::from).collect();
    }
    println!("entity_kinds: {:?}", entity_kinds);

    let extrent_rpc_addr_str: String = env::var("EXTRENT_RPC_ADDR").unwrap_or("127.0.0.1:8090".to_string());
    let extrent_rpc_connect_str = format!("http://{}", extrent_rpc_addr_str);

    let return_sentence_str : String = env::var("EXTRENT_RETURN_SEQUENCE").unwrap_or(String::from("0"));
    let return_sentence : bool = return_sentence_str.parse().unwrap_or(0) != 0;

    let mut client : EntityExtractorClient<tonic::transport::Channel>
        = EntityExtractorClient::connect(extrent_rpc_connect_str.clone()).await?;

    println!("calling extract_entity on {} ...", extrent_rpc_connect_str);

    let response = client
        .extract_entity(Request::new(ExtractEntityRequest {
            text: text,
            entity_kinds: entity_kinds,
            return_sentence: return_sentence,
        }))
        .await?;
    println!("RESPONSE:\n{response:#?}\n");

    Ok(())
}

