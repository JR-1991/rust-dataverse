use std::collections::HashMap;

use typify::import_types;

use crate::client::{evaluate_response, BaseClient};
use crate::request::RequestType;
use crate::response::Response;
use crate::search_api::query::SearchQuery;

import_types!(schema = "models/search.json");

/// Performs a search operation against the Dataverse API.
///
/// This asynchronous function sends a GET request to the search API endpoint with the
/// provided search query parameters. It supports various search options including
/// filtering by type, sorting, pagination, and faceting.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `query` - A reference to the `SearchQuery` containing all search parameters.
///
/// # Returns
///
/// A `Result` wrapping a `Response<SearchResponse>`, which contains the HTTP response status
/// and the deserialized search results if the request is successful, or a `String` error
/// message on failure.
pub async fn search(
    client: &BaseClient,
    query: &SearchQuery,
) -> Result<Response<SearchResponse>, String> {
    // Endpoint metadata
    let url = "api/search";
    let params: HashMap<String, String> = query.into();

    let context = RequestType::Plain {};
    let response = client.get(url, params.into(), context, None).await;

    evaluate_response::<SearchResponse>(response).await
}
