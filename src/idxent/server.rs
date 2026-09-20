use std::env;
/*
use reqwest;
use html2text;
use rusty::*;*/

use axum::{Router, routing::{get, put}, extract::{State,Path}, Json, http::StatusCode};
use url::Url;
use urlnorm::UrlNormalizer;
use std::sync::{Arc};
use tokio::{time::{Duration}, sync::{RwLock,Mutex,MutexGuard}, task::JoinHandle};
use std::option::Option;

use std::collections::BTreeSet;
use super::config::Config;

#[derive(Debug, Clone, Default)]
pub struct SharedState
{
    config : Arc<RwLock<Config>>, //configuration, guarded by RwLock from simultaneous read and write
    idx_task: Arc<Mutex<Option<JoinHandle<()>>>> // handle of the spawned indexing task
}

pub struct Server
{
    router : Router,
    shared_state : SharedState
}

impl Server
{
    pub fn new() -> Self
    {
        let shared_state = SharedState {
            config : Arc::new(RwLock::new( Config::load().unwrap_or_default() )), 
            idx_task : Arc::new(Mutex::new(None))
        };

        // define the handlers for the served routes
        let router = Router::new()
        // paths, defining and controlling the list of sites to index
        .route("/index/sites", get(sites_list).delete(sites_clear))
        .route("/index/sites/{*site_url}", put(site_put).delete(site_delete))
            // start/stop indexing task
        .route("/index/start", get(index_start))
        .route("/index/stop", get(index_stop))
        .with_state(shared_state.clone());

        Server {
            router : router,
            shared_state : shared_state.clone()
        }
    }

    pub fn get_router(&self)->&Router
    {
        &self.router
    }
}


// reports the list of sites to be indexed
async fn sites_list(
    State(state): State<SharedState>
) -> Result<Json<Vec<String>>, (StatusCode, String)>
{
    println!("sites_list");
    match state.config.try_read()
    {
        Ok(config) => {
            let sites : &BTreeSet<String> = &config.sites;
            let sites_vec = sites.iter().map(|s|s.to_string()).collect::<Vec<_>>();
            Ok(Json(sites_vec)) 
        },
        Err(_err) => {
            Err((StatusCode::LOCKED, String::from("config locked"))) 
        }
    }
}

// clears the list of sites to be indexed
async fn sites_clear(
    State(state): State<SharedState>
) -> Result<Json<Vec<String>>, (StatusCode, String)>
{
    println!("sites_clear");
    match state.config.try_write()
    {
        Ok(mut config_lock) => { 
            let config : &mut Config = &mut *config_lock;
            let sites: &mut BTreeSet<String> = &mut config.sites;
            let sites_vec: Vec<String> = sites.iter().map(|s|s.to_string()).collect::<Vec<_>>();
            sites.clear();
            config.save();
            Ok(Json(sites_vec)) // return the list of deleted urls
        },
        Err(_err) => { 
            return Err((StatusCode::LOCKED, String::from("sites_clear failed: config locked")))
        }
    }
}

// add URL of site into the list of sites to be indexed
async fn site_put(
    State(state): State<SharedState>, Path(site_url):Path<String>
) -> Result<Json<String>, (StatusCode, String)>
{
    println!("site_put: site_url={}", site_url);
    let url_parsed = match Url::parse(site_url.as_str())
    { 
        Ok(url) => 
            if url.scheme().len() != 0 && url.scheme() != "http" && url.scheme() != "https" 
            { return Err((StatusCode::BAD_REQUEST, format!("Not a http(s) scheme in '{site_url}'."))) }
            else if url.cannot_be_a_base()
            { return Err((StatusCode::BAD_REQUEST, format!("Url '{site_url}' cannot be a base."))) }
            else { url },
        Err(err) => return Err((StatusCode::BAD_REQUEST, format!("Failed to parse site_url='{site_url}' as url: {err}")))
    };
    // normalize - remove empty path chunks, leading to duplicating slashes
    let url_normalizer = UrlNormalizer::default();
    let site_url_path = url_normalizer.compute_normalization_string(&url_parsed).replace(":","/");

    match state.config.try_write()
    {
        Ok(mut config_lock) => { 
            let config : &mut Config = &mut *config_lock;
            let sites: &mut BTreeSet<String> = &mut config.sites;
            sites.insert(site_url_path.clone());
            config.save();
            Ok(Json(site_url_path)) // report url path as it is stored in the list of sites to index
        },
        Err(_err) => { 
            return Err((StatusCode::LOCKED, String::from("site_put failed: config locked")))
        }
    }
}

// remove URL of site from the list of sites to be indexed
async fn site_delete(
    State(state): State<SharedState>, Path(site_url): Path<String>
) -> Result<Json<String>, (StatusCode, String)>
{
    println!("site_delete: site_url={}", site_url);
    let url_parsed = match Url::parse(site_url.as_str())
    { 
        Ok(url) => url,
        Err(err) => return Err((StatusCode::BAD_REQUEST, format!("Failed to parse site_url='{site_url}' as url: {err}")))
    };
    let url_normalizer = UrlNormalizer::default();
    let site_url_path = url_normalizer.compute_normalization_string(&url_parsed).replace(":","/");

    match state.config.try_write()
    {
        Ok(mut config_lock) => { 
            let config : &mut Config = &mut *config_lock;
            let sites: &mut BTreeSet<String> = &mut config.sites;
            if sites.contains(&site_url_path)
            {
                sites.remove(&site_url);
                config.save();
                Ok(Json(site_url_path)) // report deleted site url
            }
            else
            {
                Err((StatusCode::NOT_FOUND, format!("DELETE failed: site url '{site_url}' not in the index list.")))
            }
        },
        Err(_err) => { 
            return Err((StatusCode::LOCKED, String::from("site_delete failed: config locked")))
        }
    }
}

#[axum::debug_handler]
async fn index_start(
    State(state): State<SharedState>
) -> Result<Json<String>, (StatusCode, String)>
{
    let mut lock_guard: MutexGuard<'_,Option<JoinHandle<()>>> = state.idx_task.lock().await;
    match *lock_guard
    {
        Some(_) => {
            let msg = "Indexing task is already running.";
            println!("{msg}");
            return Err((StatusCode::BAD_REQUEST, String::from(msg)))
        },
        None => {
            println!("index_start: starting indexing task ...");
            
            let state_clone = state.clone();
            let idx_task_handle: JoinHandle<()> = tokio::task::spawn( async move { index_task(state_clone).await } );
            *lock_guard = Some(idx_task_handle);

            let msg = "index_start: started indexing task";
            println!("{msg}");
            Ok(Json(String::from(msg)))
        }
    }
}

async fn index_task(state: SharedState)
{
    println!("index_task: start");
    // taking Read Lock to lock from changes the configuration, with the list of sites to index in it
    let sites : &BTreeSet<String> = &state.config.read().await.sites;
    let idx_time_step_sec_str:String = env::var("IDX_TIME_STEP_SEC").unwrap_or_default();
    let idx_time_step_sec:u64 = idx_time_step_sec_str.parse().unwrap_or(15);

    for site in sites
    {
        println!("index_task: site {site}");
        tokio::time::sleep(Duration::from_secs(idx_time_step_sec)).await;
    }
    println!("index_task: stop");

    // drop the handle of indexing task (which we store in order to implement abort function)
    let mut lock_guard: MutexGuard<'_,Option<JoinHandle<()>>> = state.idx_task.lock().await;
    match &*lock_guard
    {
        Some(_idx_task) => {
            println!("index_task: dropping indexing task handle ...");
            *lock_guard = None;
            println!("index_task: dropped indexing task handle");
        },
        None => {}
    }
}


#[axum::debug_handler]
async fn index_stop(
    State(state): State<SharedState>
) -> Result<Json<String>, (StatusCode, String)>
{
    let mut lock_guard: MutexGuard<'_,Option<JoinHandle<()>>> = state.idx_task.lock().await;
    match &*lock_guard
    {
        Some(idx_task) => {
            println!("index_stop: stopping indexing task ...");
            idx_task.abort();
            let msg = "index_stop: stopped indexing task";

            println!("index_stop: dropping indexing task handle ...");
            *lock_guard = None;
            println!("index_stop: dropped indexing task handle");

            println!("{msg}");
            Ok(Json(String::from(msg)))
        }
        None => {
            let msg = "index_stop: cannot stop: indexing task is not running.";
            println!("{msg}");
            Err((StatusCode::BAD_REQUEST, String::from(msg)))
        }
    }
}


/*
async fn test_load_page() -> Result<(), Box<dyn std::error::Error>>
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
*/

// Fetches page HTML over HTTP
// Fails early on non-2xx responses
/*
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
*/
