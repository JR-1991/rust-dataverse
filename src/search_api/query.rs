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
    /// This is used to handle cases where the field name in the struct differs from
    /// the parameter name expected by the API.
    static ref KEY_MAPPINGS: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("type", "search_type");
        m
    };
}

/// Macro to insert a field into the parameters map if it is `Some`.
///
/// # Arguments
///
/// * `$obj` - The object containing the field
/// * `$field` - The field name to check and insert
/// * `$params` - The parameters map to insert into
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
/// This handles fields that are vectors, joining them with the appropriate separator.
///
/// # Arguments
///
/// * `$obj` - The object containing the field
/// * `$field` - The field name to check and insert
/// * `$params` - The parameters map to insert into
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
///
/// This struct encapsulates all possible parameters that can be used when
/// searching a Dataverse instance. It provides a flexible way to construct
/// search queries with different filtering, sorting, and pagination options.
#[derive(Debug, Serialize, Deserialize, StructOpt, Default)]
#[structopt(about = "Search a Dataverse instance")]
pub struct SearchQuery {
    /// The search query string. This is the main search term.
    #[structopt(short = "q", long = "query", help = "The search query string")]
    pub q: String,

    /// The type of search to perform (dataverse, dataset, or file).
    #[structopt(short = "t", long = "type", help = "The type of search")]
    pub search_type: Option<Vec<SearchType>>, // Represents "type" in the query parameters

    /// The subtree to search within, limiting results to a specific dataverse.
    #[structopt(long, help = "The subtree to search within")]
    pub subtree: Option<Vec<String>>,

    /// The field to sort results by (name or date).
    #[structopt(long, help = "The field to sort by")]
    pub sort: Option<SortField>,

    /// The order of sorting (ascending or descending).
    #[structopt(long, help = "The order of sorting")]
    pub order: Option<Order>,

    /// The number of results to return per page.
    #[structopt(long, help = "The number of results per page")]
    pub per_page: Option<u32>,

    /// The starting index of the results for pagination.
    #[structopt(long, help = "The starting index of the results")]
    pub start: Option<u32>,

    /// Whether to show relevance scores in the search results.
    #[structopt(long, help = "Whether to show relevance scores")]
    pub show_relevance: Option<bool>,

    /// Whether to show facets in the search results.
    #[structopt(long, help = "Whether to show facets")]
    pub show_facets: Option<bool>,

    /// Filter queries to narrow down search results.
    #[structopt(long = "filter", help = "The filter query")]
    pub fq: Option<Vec<String>>,

    /// Whether to show entity IDs in the search results.
    #[structopt(long, help = "Whether to show entity IDs")]
    pub show_entity_ids: Option<bool>,

    /// The geographic point for geo-spatial searches.
    #[structopt(long, help = "The geographic point")]
    pub geo_point: Option<String>,

    /// The geographic radius for geo-spatial searches.
    #[structopt(long, help = "The geographic radius")]
    pub geo_radius: Option<String>,

    /// The metadata fields to search within, limiting the search scope.
    #[structopt(long = "fields", help = "The metadata fields to search within")]
    pub metadata_fields: Option<Vec<String>>,
}

impl SearchQuery {
    /// Converts the `SearchQuery` instance into a map of query parameters.
    ///
    /// This method transforms all the fields of the SearchQuery into a format
    /// that can be used as HTTP query parameters when making API requests.
    /// It handles both required and optional fields, and uses the KEY_MAPPINGS
    /// to ensure field names match the expected API parameter names.
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

/// Implements conversion from a SearchQuery reference to a HashMap.
/// This allows for easy conversion when preparing API requests.
impl From<&SearchQuery> for HashMap<String, String> {
    fn from(query: &SearchQuery) -> Self {
        query.to_query_params()
    }
}

/// Implements string parsing for SearchQuery.
/// This allows creating a basic search query from just a search string.
impl FromStr for SearchQuery {
    type Err = String;

    /// Creates a new SearchQuery with the given string as the query term.
    /// All other fields are set to their default values.
    ///
    /// # Arguments
    /// * `s` - The search query string
    ///
    /// # Returns
    /// A Result containing the new SearchQuery or an error message
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SearchQuery {
            q: s.to_string(),
            ..Default::default()
        })
    }
}

/// Implements the Matcher trait for SearchQuery.
/// This allows the search query to be processed by the CLI framework.
impl Matcher for SearchQuery {
    /// Processes the search query by sending it to the API and handling the response.
    ///
    /// # Arguments
    /// * `self` - The SearchQuery to process
    /// * `client` - The BaseClient to use for the API request
    fn process(self, client: &BaseClient) {
        let runtime = Runtime::new().unwrap();
        let response = runtime.block_on(search_api::search(client, &self));
        evaluate_and_print_response(response);
    }
}

/// Represents the type of search.
///
/// This enum defines the different entity types that can be searched in a Dataverse instance:
/// - Dataverse: Search for dataverse containers
/// - Dataset: Search for datasets
/// - File: Search for individual files
#[derive(Debug, Serialize, Deserialize)]
pub enum SearchType {
    /// Search for dataverse containers
    Dataverse,
    /// Search for datasets
    Dataset,
    /// Search for individual files
    File,
}

impl FromStr for SearchType {
    type Err = String;

    /// Converts a string to a `SearchType` enum.
    ///
    /// This function parses a string representation of a search type and
    /// returns the corresponding enum variant. The parsing is case-insensitive.
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
///
/// This enum defines the available fields that can be used for sorting search results:
/// - Name: Sort by name
/// - Date: Sort by date
#[derive(Debug, Serialize, Deserialize)]
pub enum SortField {
    /// Sort by name
    Name,
    /// Sort by date
    Date,
}

impl FromStr for SortField {
    type Err = String;

    /// Converts a string to a `SortField` enum.
    ///
    /// This function parses a string representation of a sort field and
    /// returns the corresponding enum variant. The parsing is case-insensitive.
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
///
/// This enum defines the available sort orders:
/// - Asc: Ascending order (A-Z, oldest to newest)
/// - Desc: Descending order (Z-A, newest to oldest)
#[derive(Debug, Serialize, Deserialize)]
pub enum Order {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

impl FromStr for Order {
    type Err = String;

    /// Converts a string to an `Order` enum.
    ///
    /// This function parses a string representation of a sort order and
    /// returns the corresponding enum variant. The parsing is case-insensitive.
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

/// Implements string representation for SearchType.
/// This is used when converting the enum to a query parameter string.
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

/// Implements string representation for SortField.
/// This is used when converting the enum to a query parameter string.
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

/// Implements string representation for Order.
/// This is used when converting the enum to a query parameter string.
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
