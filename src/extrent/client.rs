use std::env;
use std::fs;

use tonic::Request;

use extrent::entity_extractor_client::EntityExtractorClient;
use extrent::{ExtractEntityRequest};

pub mod extrent
{
    include!("generated/extrent.rs");
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // read text from file
    let mut text : String = "".to_string();
    let source_file_var = env::var("SOURCE_FILE");
    if source_file_var.is_ok()
    {
        let source_file = source_file_var.unwrap();
        println!("SOURCE_FILE: {}", source_file);
        text = fs::read_to_string(&source_file).unwrap_or_else(
            |error|{ panic!("Failed reading from file {}: {}", source_file, error) } );
    //  println!("TEXT FROM SOURCE_FILE={}:\n{}", source_file, text);
    }

    let mut entity_types : Vec<String> = vec![];
    let entity_types_var = env::var("ENTITY_TYPES");
    if entity_types_var.is_ok()
    {
        let entity_types_str : String = entity_types_var.unwrap();
        entity_types = entity_types_str.split(",").map(String::from).collect();
    }
    println!("entity_types: {}", entity_types.join(":"));

    let extrent_rpc_addr_str: String = env::var("EXTRENT_RPC_ADDR").unwrap_or("[::1]:10000".to_string());
    let extrent_rpc_connect_str = format!("http://{}", extrent_rpc_addr_str);

    let return_sentence_str : String = env::var("EXTRENT_RETURN_SEQUENCE").unwrap_or(String::from("0"));
    let return_sentence : bool = return_sentence_str.parse().unwrap_or(0) != 0;

    let mut client = EntityExtractorClient::connect(extrent_rpc_connect_str).await?;

    println!("*** SIMPLE RPC: extract_entity ***");

    let response = client
        .extract_entity(Request::new(ExtractEntityRequest {
            text: text,
            entity_types: entity_types,
            return_sentence: return_sentence,
        }))
        .await?;
    println!("RESPONSE = {response:?}");

    Ok(())
}

