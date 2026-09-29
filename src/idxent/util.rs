use url::{Url,Position,ParseError};
use select::document::Document;
use select::predicate::Name;


// extract links from a html document and returns whose which point within specified site
pub fn get_page_links(doc_html : &String, doc_url : &Url, site_url : &Url) -> Vec<Url>
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
pub fn get_doc_base_url(doc: &Document, doc_url: &Url) -> Option<Url>
{
    let base_tag_href = doc.find(Name("base")).filter_map(|n| n.attr("href")).nth(0);
    base_tag_href.map_or_else(|| Url::parse(&doc_url[..Position::AfterPath]), Url::parse).ok()
}

pub fn validate_site_url(site_url : &str) -> anyhow::Result<Url>
{
    let url_parse_result : Result<Url, ParseError> = Url::parse(site_url);
    if url_parse_result.is_err() 
    { 
        Err(anyhow::Error::new(url_parse_result.unwrap_err()))
    } 
    else
    {
        let url : Url = url_parse_result.unwrap();
        if !url_is_http(&url)
        {
            return Err(anyhow::Error::msg("not a http(s) url: '{url}'"));
        }
        else if url.cannot_be_a_base()
        {
            return Err(anyhow::Error::msg(format!("URL '{url}' cannot be a base")));
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
pub fn get_site_url_path(url_site_start_page: &Url) -> anyhow::Result<Url>
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
            Err(parse_error) => Err(anyhow::Error::new(parse_error))
        }
    }
}

pub fn url_is_http(url: &Url) -> bool
{
    url.scheme() == "http" || url.scheme() == "https"
}

pub fn url_no_fragment(url: &Url) -> Url
{
    Url::parse(&url[..Position::AfterQuery]).unwrap_or(url.clone())
}

pub fn url_before_query(url: &Url) -> Url
{
    Url::parse(&url[..Position::BeforeQuery]).unwrap_or(url.clone())
}

