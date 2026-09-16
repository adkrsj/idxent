use std::env;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use findent::entity_finder_server::{EntityFinder, EntityFinderServer};
use findent::{FindEntityRequest, NamedEntityRef, FindEntityResponse };

use rusty::{Language};

pub mod findent
{
    include!("generated/findent.rs");
}


#[derive(Debug)]
pub struct EntityFinderService
{
    rusty_nlp_model: Arc<rusty::Language>,
}

// accepts an expression of type Result<T, SpaCyError>, returned from rusTy APIs;
// if it is Ok - returns it, otherwise generates a return call for server gRPC method impl,
// with SpaCyError wrapped into its error Status.
macro_rules! handle_rusty_result_error
{
    ($rusty_result_expr:expr) =>
    {
        match $rusty_result_expr
        {
            Ok(ok_result) => ok_result,
            Err(spacy_error) => return Err(Status::from_error(Box::new(spacy_error)))
        }
    };
}

#[tonic::async_trait]
impl EntityFinder for EntityFinderService
{
    async fn find_entity(&self, request: Request<FindEntityRequest>) -> Result<Response<FindEntityResponse>, Status> {
        println!("find_entity = {:?}", request);

        let text : &str = request.get_ref().text.as_str();
        let ent_types : &Vec<String> = &request.get_ref().entity_types;
        let return_sentence : bool = request.get_ref().return_sentence;
        let rusty_doc: rusty::Doc = handle_rusty_result_error!(self.rusty_nlp_model.nlp(text));
        let rusty_ents: Vec<rusty::Span> = handle_rusty_result_error!(rusty_doc.ents());

        // select named entities of types listed in request
        let entity_refs: Vec<NamedEntityRef> = rusty_ents.iter()
            .filter(|rusty_ent|
                    ent_types.is_empty() || 
                    ent_types.iter().any(|ent_type|*ent_type == rusty_ent.label_().unwrap_or_default()))
            .map(|rusty_ent|
                NamedEntityRef {
                    entity_type : rusty_ent.label_().unwrap_or_default(),
                    entity_value : rusty_ent.text().unwrap_or_default(),
                    sentence : if return_sentence && rusty_ent.sent().is_ok() 
                        { Some( rusty_ent.sent().unwrap().text().unwrap_or_default() ) } 
                        else { None },
                }
            ).collect();

        let find_entity_response = FindEntityResponse{ entity_refs : entity_refs };

        Ok(Response::new(find_entity_response))
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    // using rusTy, rust wrapper of spaCy NLP engine
    let spacy_model_name = env::var("SPACY_MODEL_NAME").unwrap_or("en_core_web_sm".to_string());
    println!("spacy_model_name: {spacy_model_name}");
    let nlp = Language::load(spacy_model_name.as_str()).unwrap_or_else(
        |error|{ panic!("Failed loading spaCy model {}: {}", spacy_model_name, error) } );
    println!("Loaded spaCy model: {spacy_model_name}");

    let findent_rpc_addr_str: String = env::var("FINDENT_RPC_ADDR").unwrap_or("[::1]:10000".to_string());
    let findent_rpc_addr : std::net::SocketAddr = findent_rpc_addr_str.parse().unwrap(); 
    println!("EntityFinderServer listening on: {findent_rpc_addr}");

    let entity_finder = EntityFinderService { rusty_nlp_model : Arc::new(nlp) };

    let svc = EntityFinderServer::new(entity_finder);

    Server::builder().add_service(svc).serve(findent_rpc_addr).await?;

    Ok(())
}

