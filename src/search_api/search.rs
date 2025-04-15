use std::collections::HashMap;

use typify::import_types;

use crate::client::{BaseClient, evaluate_response};
use crate::request::RequestType;
use crate::response::Response;
use crate::search_api::query::SearchQuery;

import_types!(schema = "models/search.json");

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
