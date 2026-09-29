// module describes Rust structs/types, corresponding to PostgreSQL table records/types

#[derive(Debug, PartialEq, sqlx::Type)]
#[sqlx(type_name = "site_status")] // for PostgreSQL to match a type definition
#[sqlx(rename_all = "lowercase")]
pub enum SiteStatus {Indexing, Indexed, Failed }

#[derive(Debug, sqlx::Type, sqlx::FromRow)]
pub struct Site
{
    pub id : i32,
    pub url : String,
    pub status : SiteStatus,
    pub status_ts : chrono::DateTime<chrono::Local>,
    pub last_error : String
}

#[derive(Debug, sqlx::Type, sqlx::FromRow)]
pub struct Page
{
    pub id : i32,
    pub url : String,
    pub site_id : i32
}

#[derive(Debug, sqlx::Type, sqlx::FromRow)]
pub struct Entity
{
    pub id : i32,
    pub page_id : i32,
    pub kind : String,
    pub value : String,
    pub sentence : String
}
