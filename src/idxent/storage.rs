use sqlx::{postgres::{PgPool,PgQueryResult}};
use super::types::{SiteStatus, Site, Page, Entity};
use chrono::prelude::*;

// The module provides access to db (PostgreSQL)

// return number of deleted rows
pub async fn delete_site(pool : &PgPool, url: &str) -> sqlx::Result<u64>
{
    // cascade delete of site with referencing pages and entities
    let query_result : Result<PgQueryResult, sqlx::Error>
        = sqlx::query("DELETE FROM sites WHERE url=$1")
        .bind(url)
        .execute(pool)
        .await;

    match query_result
    {
        Ok(query_result) => Ok(query_result.rows_affected()),
        Err(err) => Err(err) 
    }
}

pub async fn create_site(pool : &PgPool, url : &str) -> sqlx::Result<Site>
{
    let site_created : Site = sqlx::query_as(
        r#"
            INSERT INTO sites (url, status, status_ts, last_error)
            VALUES ($1, $2, $3, $4)
            RETURNING id, url, status, status_ts, last_error
        "#)
        .bind(url)
        .bind(SiteStatus::Indexing)
        .bind(Local::now())
        .bind("")
        .fetch_one(pool)
        .await?;
    Ok(site_created)
}

pub async fn update_site_status(
    pool : &PgPool, 
    site_id: i32,
    site_status: SiteStatus,
    last_error : &str
) -> sqlx::Result<Site>
{
    let site_updated : Site = sqlx::query_as(
        r#"
            UPDATE sites SET status=$2, status_ts=$3, last_error=$4
            WHERE id=$1
            RETURNING id, url, status, status_ts, last_error
        "#)
        .bind(site_id)
        .bind(site_status)
        .bind(Local::now())
        .bind(last_error)
        .fetch_one(pool)
        .await?;
    Ok(site_updated)
}

pub async fn get_site(pool : &PgPool, url : &str) -> sqlx::Result<Site>
{
    let site : Site = sqlx::query_as(
        r#"
            SELECT id, url, status, status_ts, last_error FROM sites
            WHERE url=$1
        "#)
        .bind(url)
        .fetch_one(pool)
        .await?;
    Ok(site)
}


pub async fn create_page(pool : &PgPool, page_url : &str, site_id : i32) -> sqlx::Result<Page>
{
    let page_created : Page = sqlx::query_as(
        r#"
            INSERT INTO pages (url, site_id)
            VALUES ($1, $2)
            RETURNING id, url, site_id
        "#)
        .bind(page_url)
        .bind(site_id)
        .fetch_one(pool)
        .await?;
    Ok(page_created)
}

pub async fn get_page(pool : &PgPool, page_id : i32) -> sqlx::Result<Page>
{
    let page : Page = sqlx::query_as(
        r#"
            SELECT id, url, site_id FROM pages
            WHERE id=$1
        "#)
        .bind(page_id)
        .fetch_one(pool)
        .await?;
    Ok(page)        
}

pub async fn create_entity(
    pool : &PgPool, 
    page_id : i32, 
    ent_kind : &str, 
    ent_value : &str,
    sentence : &str
) -> sqlx::Result<Entity>
{
    let entity_created : Entity = sqlx::query_as(
        r#"
            INSERT INTO entities (page_id, kind, value, sentence)
            VALUES ($1, $2, $3, $4)
            RETURNING id, page_id, kind, value, sentence
        "#)
        .bind(page_id)
        .bind(ent_kind)
        .bind(ent_value)
        .bind(sentence)
        .fetch_one(pool)
        .await?;
    Ok(entity_created)
}

