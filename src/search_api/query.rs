use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use structopt::lazy_static::lazy_static;
use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::cli::base::{evaluate_and_print_response, Matcher};
use crate::client::BaseClient;
use crate::search_api;

lazy_static! {
    /// A static mapping of key names to their corresponding query parameter names.
    static ref KEY_MAPPINGS: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("type", "search_type");
        m
    };
}

/// Macro to insert a field into the parameters map if it is `Some`.
macro_rules! insert_if_some {
    ($obj:expr, $field:ident, $params:expr) => {
        if let Some(value) = &$obj.$field {
            let key = KEY_MAPPINGS
                .get(stringify!($field))
                .unwrap_or(&stringify!($field))
                .to_string();
            $params.insert(key, value.to_string());
        }
    };
}

/// Macro to insert multiple values of a field into the parameters map if it is `Some`.
macro_rules! insert_multiple_if_some {
    ($obj:expr, $field:ident, $params:expr) => {
        if let Some(values) = &$obj.$field {
            let key = KEY_MAPPINGS
                .get(stringify!($field))
                .unwrap_or(&stringify!($field))
                .to_string();
            let join_pattern = format!("&{}=", key);
            let value = values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(&join_pattern);
            $params.insert(key, value);
        }
    };
}

/// Represents a search query with various optional parameters.
#[derive(Debug, Serialize, Deserialize, StructOpt, Default)]
#[structopt(about = "Search a Dataverse instance")]
pub struct SearchQuery {
    #[structopt(short = "q", long = "query", help = "The search query string")]
    pub q: String,

    #[structopt(short = "t", long = "type", help = "The type of search")]
    pub search_type: Option<Vec<SearchType>>, // Represents "type" in the query parameters

    #[structopt(long, help = "The subtree to search within")]
    pub subtree: Option<Vec<String>>,

    #[structopt(long, help = "The field to sort by")]
    pub sort: Option<SortField>,

    #[structopt(long, help = "The order of sorting")]
    pub order: Option<Order>,

    #[structopt(long, help = "The number of results per page")]
    pub per_page: Option<u32>,

    #[structopt(long, help = "The starting index of the results")]
    pub start: Option<u32>,

    #[structopt(long, help = "Whether to show relevance scores")]
    pub show_relevance: Option<bool>,

    #[structopt(long, help = "Whether to show facets")]
    pub show_facets: Option<bool>,

    #[structopt(long = "filter", help = "The filter query")]
    pub fq: Option<Vec<String>>,

    #[structopt(long, help = "Whether to show entity IDs")]
    pub show_entity_ids: Option<bool>,

    #[structopt(long, help = "The geographic point")]
    pub geo_point: Option<String>,

    #[structopt(long, help = "The geographic radius")]
    pub geo_radius: Option<String>,

    #[structopt(long = "fields", help = "The metadata fields to search within")]
    pub metadata_fields: Option<Vec<String>>,
}

impl SearchQuery {
    /// Converts the `SearchQuery` instance into a map of query parameters.
    ///
    /// # Returns
    /// A `HashMap` containing the query parameters as key-value pairs.
    pub fn to_query_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();

        // Required fields
        params.insert("q".to_string(), self.q.clone());

        // Optional fields
        insert_multiple_if_some!(self, search_type, params);
        insert_multiple_if_some!(self, subtree, params);
        insert_if_some!(self, sort, params);
        insert_if_some!(self, order, params);
        insert_if_some!(self, per_page, params);
        insert_if_some!(self, start, params);
        insert_if_some!(self, show_relevance, params);
        insert_if_some!(self, show_facets, params);
        insert_multiple_if_some!(self, fq, params);
        insert_if_some!(self, show_entity_ids, params);
        insert_if_some!(self, geo_point, params);
        insert_if_some!(self, geo_radius, params);
        insert_multiple_if_some!(self, metadata_fields, params);

        params
    }
}

impl From<&SearchQuery> for HashMap<String, String> {
    fn from(query: &SearchQuery) -> Self {
        query.to_query_params()
    }
}

impl FromStr for SearchQuery {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SearchQuery {
            q: s.to_string(),
            ..Default::default()
        })
    }
}

impl Matcher for SearchQuery {
    fn process(self, client: &BaseClient) {
        let runtime = Runtime::new().unwrap();
        let response = runtime.block_on(search_api::search(&client, &self));
        evaluate_and_print_response(response);
    }
}

/// Represents the type of search.
#[derive(Debug, Serialize, Deserialize)]
pub enum SearchType {
    Dataverse,
    Dataset,
    File,
}

impl FromStr for SearchType {
    type Err = String;

    /// Converts a string to a `SearchType` enum.
    ///
    /// # Arguments
    /// * `input` - The input string to convert.
    ///
    /// # Returns
    /// A `Result` containing the `SearchType` or an error message.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "dataverse" => Ok(SearchType::Dataverse),
            "dataset" => Ok(SearchType::Dataset),
            "file" => Ok(SearchType::File),
            _ => Err(format!("Invalid search type: {}", input)),
        }
    }
}

/// Represents the field to sort by.
#[derive(Debug, Serialize, Deserialize)]
pub enum SortField {
    Name,
    Date,
}

impl FromStr for SortField {
    type Err = String;

    /// Converts a string to a `SortField` enum.
    ///
    /// # Arguments
    /// * `input` - The input string to convert.
    ///
    /// # Returns
    /// A `Result` containing the `SortField` or an error message.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "name" => Ok(SortField::Name),
            "date" => Ok(SortField::Date),
            _ => Err(format!("Invalid sort field: {}", input)),
        }
    }
}

/// Represents the order of sorting.
#[derive(Debug, Serialize, Deserialize)]
pub enum Order {
    Asc,
    Desc,
}

impl FromStr for Order {
    type Err = String;

    /// Converts a string to an `Order` enum.
    ///
    /// # Arguments
    /// * `input` - The input string to convert.
    ///
    /// # Returns
    /// A `Result` containing the `Order` or an error message.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "asc" => Ok(Order::Asc),
            "desc" => Ok(Order::Desc),
            _ => Err(format!("Invalid order: {}", input)),
        }
    }
}

// Implement fmt::Display for each enum for conversion to strings in HashMap
impl fmt::Display for SearchType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SearchType::Dataverse => "dataverse",
                SearchType::Dataset => "dataset",
                SearchType::File => "file",
            }
        )
    }
}

impl fmt::Display for SortField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SortField::Name => "name",
                SortField::Date => "date",
            }
        )
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Order::Asc => "asc",
                Order::Desc => "desc",
            }
        )
    }
}
