use std::env;
use std::option::Option;
use std::collections::{BTreeSet,BTreeMap};
use std::sync::{Arc};

use anyhow::Context;

use tokio::{sync::{RwLock,Mutex,MutexGuard,mpsc}, task::JoinHandle};
use axum::{Router, routing::{get, put}, extract::{State,Path}, Json, http::StatusCode};

use url::Url;
use reqwest;
use deformat::extract;

use sqlx::postgres::PgPool;

use super::config::Config;
use super::util::*;
use super::types::{SiteStatus, Site, Page, Entity};
use super::storage as storage;

// extract entity service includes
use crate::extrent::{ExtractEntityRequest, NamedEntityRef, ExtractEntityResponse};
use crate::extrent::entity_extractor_client::EntityExtractorClient;
type EntityExtractorClientImpl = EntityExtractorClient<tonic::transport::Channel>;


#[derive(Debug, Clone)]
pub struct SharedState
{
    config : Arc<RwLock<Config>>, //configuration, guarded by RwLock from simultaneous read and write
    db_pool : Arc<sqlx::PgPool>, // pool of connectionы to postgresql db
    extrent_client : Arc<RwLock<EntityExtractorClientImpl>>, // client connected to 'extract entity' server
    idx_task: Arc<Mutex<Option<JoinHandle<()>>>>, // handle of the spawned indexing task
    idx_pages: Arc<RwLock<BTreeMap<Url, IdxPageStatus>>> // maps URLs of page being indexed into current status, to exclude concurrent/repeated processing
}

#[derive(Debug, Clone, PartialEq)]
enum IdxPageStatus
{
    New,
    Loaded,
    TextRead,
    EntitiesExtracted,
    EntitiesWritten,
    LinksProcessed,
}

pub struct Server
{
    router : Router,
    shared_state : SharedState
}

impl Server
{
    pub fn new(
        config : &Config, 
        db_pool : &PgPool,
        extrent_client : &EntityExtractorClientImpl
    ) -> Self
    {
        let shared_state = SharedState {
            config : Arc::new(RwLock::new(config.clone())), 
            db_pool : Arc::new(db_pool.clone()),
            extrent_client : Arc::new(RwLock::new(extrent_client.clone())),
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
    let url_validate_result : anyhow::Result<Url> = validate_site_url(site_url.as_str());
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

// performs indexing of all sites, listed in configuration
async fn index_task(shared_state: SharedState)
{
    println!("index_task: start");
    // taking Read Lock to lock from changes the configuration - specifically, the list of sites to index
    let sites : &BTreeSet<Url> = &shared_state.config.read().await.sites;

    {
        let mut idx_pages_lock = shared_state.idx_pages.write().await;
        let idx_pages : &mut BTreeMap<Url, IdxPageStatus> = &mut *idx_pages_lock;
        idx_pages.clear();
    }

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

    let mut index_site_tasks = Vec::<JoinHandle<()>>::new();
    for (site_url, site_start_url) in sites_urls
    {
        let shared_state_clone = shared_state.clone();
        // spawns the task of site indexing
        index_site_tasks.push(
            tokio::task::spawn(async move { 
                index_site(shared_state_clone, site_url.clone(), site_start_url.clone()).await }
            ));
    }

    // wait for all sites' indexing to finish
    for index_site_task in index_site_tasks
    {
        let _ = index_site_task.await; 
    }

    println!("index_task: stopping ...");

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

// performs indexing of pages of a single site
async fn index_site
(
    shared_state : SharedState,
    site_url : Url,
    site_start_url : Url
)
{
    const CHANNEL_SIZE_DEFAULT: usize = 16;
    let channel_size: usize = env::var("IDX_CHANNEL_SIZE")
        .unwrap_or_default().as_str()
        .parse::<usize>().unwrap_or(CHANNEL_SIZE_DEFAULT);
    println!("{site_url} index_site start: mpsc channel_size={channel_size}");

    let db_pool : &PgPool = &shared_state.db_pool;
    let site_delete_result = storage::delete_site(db_pool, site_url.as_str()).await;
    match site_delete_result
    {
        Ok(affected_rows) => println!("{site_url} deleted site in db, {affected_rows} affected rows"),
        Err(err) => println!("{site_url} failed to delete site in db, {err}")
    };

    let site_create_result : sqlx::Result<Site> = storage::create_site(db_pool, site_url.as_str()).await;
    let site : Site = match site_create_result
    {
        Ok(_site) => { println!("{site_url} created site in db with id {}", _site.id); _site },
        Err(err) => { println!("{site_url} failed to create site in db: {:?}", err); return }
    };

    // upon loading a web page, the site index task extracts it's link and explores linked pages;
    // the switch from parent to child pages happens via tokio tasks sending messages through mpsc channel;
    // the channel is created per-site; when the channel contains no more messages, the site indexing is finished
    // (if the same channel was used for all sites, then it would be difficult to say when a particular site indexing finished).
    let (tx, mut rx) = mpsc::channel(channel_size);

    let _shared_state = shared_state.clone();
    let _site_url = site_url.clone();
    let _page_url = site_start_url.clone();
    let arc_tx = Arc::<IdxPageSender>::new(tx.clone()); 

    tokio::task::spawn( async move {
        let _send_result = arc_tx.send(IdxPageMsg {
            tx: arc_tx.clone(),
            shared_state : _shared_state.clone(), 
            site_url : _site_url.clone(), 
            site_id : site.id,
            page_url : _page_url.clone(), 
            link_depth : 0 } 
        ).await;
    });

    // The `rx` half of the channel returns `None` once **all** `tx` clones drop.
    // Drop the handle owned by the current task to ensure rx.recv() returns `None`.
    drop(tx);
    while let Some(idx_page_msg) = rx.recv().await
    {
        println!("{0} <- rx.recv, rx.len={1}", idx_page_msg.page_url, rx.len());
        tokio::task::spawn(async move {index_page(&idx_page_msg).await });
    };
    println!("{site_url} index_site: mpsc channel is empty, indexing is complete");

    let get_site_result : sqlx::Result<Site> = storage::get_site(db_pool, site_url.as_str()).await;
    if let Ok(site) = get_site_result && site.status != SiteStatus::Failed
    {
        let _ = storage::update_site_status(db_pool, site.id, SiteStatus::Indexed, "").await;
    };
}

#[derive(Debug, Clone)]
struct IdxPageMsg
{
    // mspc sender: the page processor fn (index_page) via this sender 
    // sends into channel the messages, describing the linked pages to process further
    tx : Arc<IdxPageSender>, 
    shared_state : SharedState, // synchronized configuration and per-page state of processing
    site_url : Url, // context site, used to limit the links to those only within site
    site_id : i32,  // site id in db
    page_url : Url, // url of page to process
    link_depth: i32 // link distance from the site initial page to the page being processed
}

type IdxPageSender = tokio::sync::mpsc::Sender<IdxPageMsg>;

async fn index_page(idx_page_msg : &IdxPageMsg)
{
    let result : anyhow::Result<()> = index_page_inner(&idx_page_msg.clone()).await;
    if let Err(err) = result
    {
        let _ = storage::update_site_status(
            &idx_page_msg.shared_state.db_pool, 
            idx_page_msg.site_id, 
            SiteStatus::Failed, 
            err.to_string().as_str()).await;
    };
}

async fn index_page_inner(idx_page_msg : &IdxPageMsg) -> anyhow::Result<()>
{
    let tx: &Arc<IdxPageSender>  = &idx_page_msg.tx;
    let shared_state: &SharedState = &idx_page_msg.shared_state;
    let db_pool : &PgPool = &shared_state.db_pool;
    let config : &Config = &*shared_state.config.read().await;
    let site_url: &Url = &idx_page_msg.site_url;
    let site_id: i32 = idx_page_msg.site_id;
    let page_url: &Url = &idx_page_msg.page_url;
    let page_depth: i32 = idx_page_msg.link_depth;

    let page_url_no_fragment: &Url = &url_no_fragment(page_url);
    if page_url_no_fragment != page_url
    {
        println!("{page_url} : url := {page_url_no_fragment}, removed URL fragments part");
    }
    let _ = report_page_status::<&str, ()>(
        shared_state, page_url_no_fragment, &IdxPageStatus::New, Ok("")).await;

    let page_load_result : anyhow::Result<String> = 
        load_page(page_url_no_fragment).await
        .context("load page");

    let html : String = report_page_status(
        shared_state, page_url_no_fragment, &IdxPageStatus::Loaded, page_load_result).await?;

    let extract_text_result : anyhow::Result<deformat::Extracted> = 
        extract_text(&html)
        .context("extract text");

    let text : String = report_page_status(
        shared_state, page_url_no_fragment, &IdxPageStatus::TextRead, extract_text_result).await?.text;
    
    let page: Page =
        storage::create_page(db_pool, page_url_no_fragment.as_str(), site_id).await
        .context("create page object in db")?;

    let extract_entity_request = tonic::Request::new(
        ExtractEntityRequest {
            text : text,
            entity_kinds : config.extrent_entity_kinds.clone(),
            return_sentence : config.extrent_return_sentence,
        });

    // extract from the text the named entities via dedicated remote service
    let extract_entity_result : anyhow::Result<tonic::Response<ExtractEntityResponse>> =
        shared_state.extrent_client.write().await
        .extract_entity(extract_entity_request).await
        .context("extract_entity rpc");

    let extract_entity_response : ExtractEntityResponse = 
        report_page_status(shared_state, page_url_no_fragment, &IdxPageStatus::EntitiesExtracted, extract_entity_result)
        .await?
        .into_inner();

    println!("{page_url_no_fragment} extracted {} named entities", extract_entity_response.entity_refs.len());

    // write extracted entities into db via bulk insert
    let create_entities_result : anyhow::Result<Vec<Entity>> = 
        storage::create_entities(db_pool, page.id, &extract_entity_response.entity_refs)
        .await
        .context("create entities in db");

    let _ = report_page_status(
        shared_state, page_url_no_fragment, &IdxPageStatus::EntitiesWritten, create_entities_result)
        .await?;

    // setup processing linked pages

    // all links, pointing within the same site
    let page_links : &Vec<Url> = &get_page_links(&html, &page_url, &site_url);
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
    let max_link_depth : i32 = config.max_link_depth;
    if max_link_depth < 0 || page_depth < max_link_depth
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
                        site_id : idx_page_msg.site_id,
                        page_url : link_url.clone(), 
                        link_depth : page_depth+1 
                    };
            let send_result = tx.send(idx_link_page_msg.clone()).await;
            println!("{0} <- send link: {1:?}", idx_link_page_msg.page_url, send_result);
        }
    };

    let _ = report_page_status::<&str, ()>(shared_state, page_url_no_fragment, &IdxPageStatus::LinksProcessed, Ok("")).await;

    Ok(())
}

async fn report_page_status<T, E>(
    shared_state : &SharedState, 
    page_url_no_fragment : &Url, 
    status_new : &IdxPageStatus,
    result : Result<T, E>
) -> Result<T, E>
where E : std::fmt::Debug
{
    let mut idx_pages_lock = shared_state.idx_pages.write().await;
    let status_old : Option<IdxPageStatus> = (*idx_pages_lock).insert(page_url_no_fragment.clone(), status_new.clone());

    if *status_new == IdxPageStatus::New
    {
        // assert that the page is NOT being/has been processed from another task
        assert!(status_old.is_none());
    }

    let err_str : String  = match result
    {
        Err(ref err) => format!(": error {err:?}") ,
        Ok(ref value) => String::from("")
    };

    let status_old_str : String = match status_old
    {
        Some(status) => format!("{:?}", status),
        None => String::from("None")
    };
    println!("{page_url_no_fragment} status {} -> {:?} {}", status_old_str, status_new, err_str);
    result
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


fn extract_text(html: &String) -> Result<deformat::Extracted, deformat::Error>
{
    return deformat::extract(html);
}

