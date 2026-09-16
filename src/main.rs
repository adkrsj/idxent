use std::env;
use std::fs;
use reqwest;
use html2text;
use rusty::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    let mut text : String = "".to_string();

    // read text from file
    let source_file_var = env::var("SOURCE_FILE");
    let source_url_var = env::var("SOURCE_URL");
    if source_file_var.is_ok()
    {
        let source_file = source_file_var.unwrap();
        println!("SOURCE_FILE: {}", source_file);
        text = fs::read_to_string(&source_file).unwrap_or_else(
            |error|{ panic!("Failed reading from file {}: {}", source_file, error) } );
        println!("TEXT FROM SOURCE_FILE={}:\n{}", source_file, text);
    }
    else if source_url_var.is_ok()
    {
        let source_url = source_url_var.unwrap();
        println!("SOURCE_URL: {}", source_url);
        let html = fetch_html(&source_url).await?;
        text = html_to_text(&html);
        println!("TEXT FROM SOURCE_URL={}:\n{}", source_url, text);
    }

    if text.len() != 0
    {
        let _ = process_text_with_spacy(&text);
    }
    else
    {
        println!("No source for text is specified.");
    }

    Ok(())
}

// Fetches page HTML over HTTP
// Fails early on non-2xx responses
async fn fetch_html(url: &str) -> Result<String, reqwest::Error> 
{
    reqwest::ClientBuilder::new()
        .user_agent(env::var("USER_AGENT").unwrap_or("Mozilla/5.0".to_string()))
        .build()? 
        .get(url) // obtain RequestBuilder
        .send() // construct the Request and sendl it to the target URL, returning a future Response
        .await?
        .error_for_status()? // turn HTTP errors into Rust errors
        .text() // read response body as text
        .await
}

fn html_to_text(html: &String) -> String
{
    html2text::from_read(html.as_bytes(), 1024).unwrap()
}

fn process_text_with_spacy(text: &String) -> Result<(), SpaCyError>
{
    let spacy_model_name = env::var("SPACY_MODEL_NAME").unwrap_or("en_core_web_sm".to_string());

    // using rusTy, rust wrapper of spaCy
    let nlp = Language::load(spacy_model_name.as_str()).unwrap_or_else(
        |error|{ panic!("Failed loading spaCy model {}: {}", spacy_model_name, error) } );

        
    let doc: rusty::Doc = nlp.nlp(text).unwrap_or_else(
        |error|{ panic!("Failed processing text: {}", error) } );

    let ents: Vec<rusty::Span> = doc.ents()?;
    println!("ENTITIES:");
    for ent in ents
    {
        println!("{}\t|\t{}\t|\t{}", ent.text()?, ent.label_()?, ent.sent()?.text()?);
    }

    Ok(())
}

