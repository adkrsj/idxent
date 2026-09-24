use std::env;
use std::fs;
use std::error::Error;
use std::option::Option;
use std::collections::{BTreeSet,BTreeMap};
use std::sync::{Arc};
use tokio::{sync::{RwLock,Mutex,MutexGuard,mpsc}, task::{spawn,JoinHandle}};
use axum::{Router, routing::{get, put}, extract::{State,Path}, Json, http::StatusCode};
use url::{Url,Position,ParseError};
use select::document::Document;
use select::predicate::Name;

use super::config::Config;

use reqwest;
use html2text;
//use rusty::*;

#[derive(Debug, Clone, Default)]
pub struct SharedState
{
    config : Arc<RwLock<Config>>, //configuration, guarded by RwLock from simultaneous read and write
    idx_task: Arc<Mutex<Option<JoinHandle<()>>>>, // handle of the spawned indexing task
    idx_pages: Arc<RwLock<BTreeMap<Url, IdxPageStatus>>>  // maps URLs of page being indexed into current status, to excluded concurrent/repeated processing
}

#[derive(Debug, Clone, PartialEq)]
enum IdxPageStatus
{
    New,
    Loaded,
    ProcessedBody,
    ProcessedLinks,
    Error,
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
            idx_task : Arc::new(Mutex::new(None)),
            idx_pages : Arc::new(RwLock::new(BTreeMap::<Url, IdxPageStatus>::new()))
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
            let sites : &BTreeSet<Url> = &config.sites;
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
            let sites: &mut BTreeSet<Url> = &mut config.sites;
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
    let url_validate_result : Result<Url, Box<dyn Error>> = validate_site_url(site_url.as_str());
    let url: Url = match url_validate_result
    { 
        Ok(url) => url,
        Err(err) => return Err((StatusCode::BAD_REQUEST, err.to_string()))
    };

    match state.config.try_write()
    {
        Ok(mut config_lock) => { 
            let config : &mut Config = &mut *config_lock;
            let sites: &mut BTreeSet<Url> = &mut config.sites;
            sites.insert(url.clone());
            config.save();
            Ok(Json(url.to_string())) // report url path as it is stored in the list of sites to index
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
    let url = match Url::parse(site_url.as_str())
    { 
        Ok(url) => url,
        Err(err) => return Err((StatusCode::BAD_REQUEST, format!("Failed to parse site url '{site_url}': {err}")))
    };

    match state.config.try_write()
    {
        Ok(mut config_lock) => { 
            let config : &mut Config = &mut *config_lock;
            let sites: &mut BTreeSet<Url> = &mut config.sites;
            if sites.contains(&url)
            {
                sites.remove(&url);
                config.save();
                Ok(Json(url.to_string())) // report deleted site url
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

async fn index_task(shared_state: SharedState)
{
    println!("index_task: start");
    // taking Read Lock to lock from changes the configuration - primarily the list of sites to index
    let sites : &BTreeSet<Url> = &shared_state.config.read().await.sites;

    const CHANNEL_SIZE_DEFAULT: usize = 100;
    let channel_size: usize = env::var("IDX_CHANNEL_SIZE")
        .unwrap_or_default().as_str()
        .parse::<usize>().unwrap_or(CHANNEL_SIZE_DEFAULT);
    println!("index_task: channel_size={channel_size}");

    // upon loading a web page, the indexer extracts it's link and explores linked pages;
    // the switch from parent to child pages happens via tokio tasks sending messages through mpsc channel
    let (tx, mut rx) = mpsc::channel(channel_size);

    // validate site's start page url and calculate site path url to filter contained site's pages among all links
    let mut sites_urls:Vec<(Url,Url)> = Vec::<(Url,Url)>::new();
    for url in sites
    {
        let site_start_page_url = match validate_site_url(url.as_str())
        {
            Ok(_url) => Some(_url),
            Err(err) => {println!("{url} : skipped unacceptable site start page url: {err}"); None }
        };
        if site_start_page_url.is_some()
        {
            let site_url : Option<Url> = get_site_url_path(&site_start_page_url.as_ref().unwrap()).ok();
            sites_urls.push( (site_url.unwrap(), site_start_page_url.unwrap()) );
        };
    }

    for (site_url, site_start_url) in sites_urls
    {
        println!("{site_url} start indexing site: ");
        let tx = tx.clone();
        let shared_state = shared_state.clone();
        let page_url = site_start_url.clone();
        tokio::task::spawn( async move {
            let send_result = tx.send(IdxPageMsg {
                tx: Arc::<IdxPageSender>::new(tx.clone()),
                shared_state : shared_state, 
                site_url : site_url.clone(), 
                page_url: page_url, 
                link_depth : 0 } 
            ).await;
            println!("{}, start page {}: <- spawn site; tx.send result: {:?}", site_url.clone(), site_start_url.clone(), send_result);
        });
    }

    // The `rx` half of the channel returns `None` once **all** `tx` clones drop.
    // Drop the handle owned by the current task to ensure rx.recv() returns `None`.
    drop(tx);
    while let Some(idx_page_msg) = rx.recv().await
    {
        println!("{0} <- rx.recv, rx.len={1}", idx_page_msg.page_url, rx.len());
        tokio::task::spawn(async move {index_page(&idx_page_msg).await });
    };
    println!("index_task: mpsc channel is empty, stopping ...");

    // drop the handle of indexing task (which we store in order to implement abort function)
    let mut lock_guard: MutexGuard<'_,Option<JoinHandle<()>>> = shared_state.idx_task.lock().await;
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

#[derive(Debug, Clone)]
struct IdxPageMsg
{
    // mspc sender: the page processor fn (index_page) via this sender 
    // sends into channel the messages, describing the linked pages to process further
    tx : Arc<IdxPageSender>, 
    shared_state : SharedState, // synchronized configuration and per-page state of processing
    site_url : Url, // context site, used to limit the links to those only within site
    page_url : Url, // url of page to process
    link_depth: u32 // link distance from the site initial page to the page being processed
}

type IdxPageSender = tokio::sync::mpsc::Sender<IdxPageMsg>;

async fn index_page(
    idx_page_msg : &IdxPageMsg)
{
    let tx: &Arc<IdxPageSender>  = &idx_page_msg.tx;
    let shared_state: &SharedState = &idx_page_msg.shared_state;
    let site_url: &Url = &idx_page_msg.site_url;
    let page_url: &Url = &idx_page_msg.page_url;
    let page_depth: u32 = idx_page_msg.link_depth;

    let page_url_no_fragment: &Url = &url_no_fragment(page_url);
    if page_url_no_fragment != page_url
    {
        println!("{page_url} : url := {page_url_no_fragment}, removed URL fragments part");
    }

    // mark this page as being processed
    let mut opt_err : Option<String> = None;
    let mut page_status = IdxPageStatus::New;
    update_page_status(shared_state, page_url_no_fragment, &page_status, &opt_err).await;

    let page_load_result : Result<String, reqwest::Error> = load_page(page_url).await;
    let mut opt_html : Option<String> = None;
    match page_load_result
    {
        Ok(html)   => { page_status = IdxPageStatus::Loaded; opt_html = Some(html); },
        Err(error) => { page_status = IdxPageStatus::Error; opt_err = Some(error.to_string()); }
    };
    update_page_status(shared_state, page_url_no_fragment, &page_status, &opt_err).await;
    if page_status == IdxPageStatus::Error
    {
        return
    }

    // TODO: extract entities, store into db, handle errors if any

    page_status = IdxPageStatus::ProcessedBody;
    update_page_status(shared_state, page_url_no_fragment, &page_status, &opt_err).await;

    // setup processing linked pages

    // all links, pointing within the same site
    let page_links : &Vec<Url> = 
        if opt_html.is_some() 
        { &get_page_links(&opt_html.unwrap(), &page_url, &site_url) } 
        else { &vec![] };
    let page_link_count : usize = page_links.len();

    // unique links among them, after discarding optional fragment suffix "#section" of url
    let page_links_unique : Vec<Url> = page_links.iter()
        .map(|url|url_no_fragment(&url))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let page_unique_link_count : usize = page_links_unique.len();

    // select unique link URLs, of which processing hasn't been started yet
    let mut page_links_unique_no_status : Vec<&Url> = vec![];
    let max_link_depth : u32 = shared_state.config.read().await.max_link_depth;
    if max_link_depth <=0 || page_depth < max_link_depth
    {
        let idx_pages_lock = shared_state.idx_pages.write().await;

        page_links_unique_no_status = page_links_unique.iter()
            .filter( |url| !(*idx_pages_lock).contains_key(&url) )
            .collect::<Vec<_>>();

        // send into the processing mspc channel the messages, which will trigger processing of the linked pages;
        // doing this with write lock on idx_pages to ensure that each link is inserted into channel only once,
        // and consequently is processed only once by a single task (spawned in index_task fn)
        println!("{page_url_no_fragment}: send msgs for links, {page_link_count} total, {page_unique_link_count} unique, {} not processed yet:", page_links_unique_no_status.len());
        for link_url in page_links_unique_no_status
        {
            let tx = tx.clone();
            let idx_link_page_msg = IdxPageMsg {
                        tx : tx.clone(),
                        shared_state : shared_state.clone(), 
                        site_url : site_url.clone(), 
                        page_url : link_url.clone(), 
                        link_depth : page_depth+1 
                    };
            let send_result = tx.send(idx_link_page_msg.clone()).await;
            println!("{0} <- send link: {1:?}", idx_link_page_msg.page_url, send_result);
        }
    };

    update_page_status(shared_state, page_url_no_fragment, &IdxPageStatus::ProcessedLinks, &opt_err).await;
}

async fn update_page_status(
    shared_state : &SharedState, 
    page_url_no_fragment : &Url, 
    status_new : &IdxPageStatus,
    opt_err : &Option<String>)
{
    let mut idx_pages_lock = shared_state.idx_pages.write().await;
    let status_old : Option<IdxPageStatus> = (*idx_pages_lock).insert(page_url_no_fragment.clone(), status_new.clone());
    if *status_new == IdxPageStatus::New
    {
        // assert that the page is NOT being/has been processed from another task 
        assert!(status_old.is_none());
    }
    let err_str : &str  = match opt_err 
    {
        Some(err) => err.as_str(),
        None => &""
    };
    let status_old_str : String = match status_old
    {
        Some(status) => format!("{:?}", status),
        None => String::from("None")
    };
    println!("{page_url_no_fragment} status {} -> {:?} {}", status_old_str, status_new, err_str);
}

// Fetches page HTML over HTTP
// Fails early on non-2xx responses
async fn load_page(url: &Url) -> Result<String, reqwest::Error> 
{
    reqwest::ClientBuilder::new()
        .user_agent(env::var("USER_AGENT").unwrap_or("Mozilla/5.0".to_string()))
        .build()? 
        .get(url.as_str()) // obtain RequestBuilder
        .send() // construct the Request and send it to the target URL, returning a future Response
        .await?
        .error_for_status()? // turn HTTP errors into Rust errors
        .text() // read response body as text
        .await
}

fn get_page_links(doc_html : &String, doc_url : &Url, site_url : &Url) -> Vec<Url>
{
    let doc: Document = Document::from(doc_html.as_str());
    let doc_base_url: Option<Url> = get_doc_base_url(&doc, doc_url);
    // parse relative URLs as relative to supplied base URL
    let url_base_parser = Url::options().base_url(doc_base_url.as_ref());
    let links: Vec<Url> = 
        doc.find(Name("a"))
        .filter_map(|node|node.attr("href"))
        .filter_map(|link|url_base_parser.parse(link).ok())
        .filter(|url|url_is_http(url))
         // only explore links within the same site (and initial path on it, if any)
        .filter(|url|url.host_str() == site_url.host_str() && url.path().starts_with(site_url.path()))
        .collect();
    links
}

// get doc's base url: take it from "base" element's href if defined,
// otherwise use the doc's own url with path part
fn get_doc_base_url(doc: &Document, doc_url: &Url) -> Option<Url>
{
    let base_tag_href = doc.find(Name("base")).filter_map(|n| n.attr("href")).nth(0);
    base_tag_href.map_or_else(|| Url::parse(&doc_url[..Position::AfterPath]), Url::parse).ok()
}

fn validate_site_url(site_url : &str) -> Result<Url, Box<dyn Error>>
{
    let url_parse_result = Url::parse(site_url);
    if url_parse_result.is_err() 
    { 
        Err(Box::new(url_parse_result.unwrap_err()))
    } 
    else
    {
        let url : Url = url_parse_result.unwrap();
        if !url_is_http(&url)
        {
            return Err(Box::<dyn Error>::from(format!("not a http(s) url: '{url}'")));
        }
        else if url.cannot_be_a_base()
        {
            return Err(Box::<dyn Error>::from(format!("URL '{url}' cannot be a base")));
        }
        Ok(url_before_query(&url))
    }
}

// Converts the given site url (which might be url of site's start page)
// into the url with directory-type path (ending with '/'),
// which would be used to check if given page url belongs or not to site via
// simply checking that the page url's path starts with the site url path.
// Example:
// url_site_start_page: http://www.example.com/topics/cheercat/index.html
// produces 
// site_url_path:       http://www.example.com/topics/cheercat/
// with which we consider belonging to site with start page url_site_start_page
// all page urls, which have the host "www.example.com" and of which path starts with "/topics/cheercat/":
// http://www.example.com/topics/cheercat/smile.html      - belongs to the site
// http://www.example.com/topics/dog/index.html           - doesn't belong to the site
pub fn get_site_url_path(url_site_start_page: &Url) -> Result<Url, Box<dyn Error>>
{
    let url_start_page_validated : Url = validate_site_url(url_site_start_page.as_str())?;
    let start_page_path : &str = url_start_page_validated.path();
    if start_page_path.ends_with('/')
    {
        Ok(url_start_page_validated)
    }
    else
    {
        // path ends with a file, like in /topics/cheercat/smile.html
        // switch from file path do directory path
        let join_result : Result<Url, ParseError> = url_start_page_validated.join(".");
        match join_result
        {
            Ok(url) => Ok(url),
            Err(parse_error) => Err(Box::new(parse_error))
        }
    }
}

fn url_is_http(url: &Url) -> bool
{
    url.scheme() == "http" || url.scheme() == "https"
}

fn url_no_fragment(url: &Url) -> Url
{
    Url::parse(&url[..Position::AfterQuery]).unwrap_or(url.clone())
}

fn url_before_query(url: &Url) -> Url
{
    Url::parse(&url[..Position::BeforeQuery]).unwrap_or(url.clone())
}


fn html_to_text(html: &String) -> String
{
    html2text::from_read(html.as_bytes(), 1024).unwrap()
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
        let html = load_page(&source_url).await?;
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

/*
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
