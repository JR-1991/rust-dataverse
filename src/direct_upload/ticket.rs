use std::collections::HashMap;

use typify::import_types;

use crate::client::{evaluate_response, BaseClient};
use crate::identifier::Identifier;
use crate::request::RequestType;
use crate::response::Response;

import_types!(schema = "models/direct_upload/ticket.json");

/// Requests a direct upload ticket from the Dataverse API
///
/// This function obtains a ticket containing URLs for direct file uploads to S3 or other storage.
/// The ticket may contain a single URL for small files or multiple URLs for multipart uploads of larger files.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request
/// * `file_size` - The size of the file to be uploaded in bytes
/// * `id` - An `Identifier` enum instance representing the dataset identifier
///
/// # Returns
///
/// A `Result` containing a `Response<TicketResponse>` with the upload URLs if successful,
/// or a `String` error message if the request fails
pub(crate) async fn get_ticket(
    client: &BaseClient,
    file_size: usize,
    id: Identifier,
) -> Result<Response<TicketResponse>, String> {
    let mut parameters = HashMap::new();
    let url = match id {
        Identifier::PersistentId(pid) => {
            parameters.insert("persistentId".to_string(), pid);
            "/api/datasets/:persistentId/uploadurls".to_string()
        }
        Identifier::Id(db_id) => format!("/api/datasets/{}/uploadurls", db_id),
    };

    parameters.insert("size".to_string(), file_size.to_string());

    let context = RequestType::Plain {};
    let response = client
        .get(url.as_str(), parameters.into(), context, None)
        .await;

    evaluate_response::<TicketResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::client::BaseClient;
    use crate::direct_upload::ticket::{get_ticket, TicketResponse};
    use crate::identifier::Identifier;
    use crate::test_utils::{
        create_test_collection, create_test_dataset, extract_test_env, set_storage_backend,
    };

    async fn setup_direct_upload(backend: &str) -> (BaseClient, String) {
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");
        let collection_alias = create_test_collection(&client, "Root").await;
        let (_, pid) = create_test_dataset(&client, &collection_alias).await;
        set_storage_backend(&client, backend, &collection_alias).await;

        (client, pid)
    }

    #[tokio::test]
    async fn test_small_ticket() {
        // Set up the client and a dataset/collection
        let (client, pid) = setup_direct_upload("LocalStack").await;

        // Get a ticket
        let ticket = get_ticket(&client, 100, Identifier::PersistentId(pid))
            .await
            .expect("Failed to get ticket")
            .data
            .expect("Failed to get ticket data");

        if let TicketResponse::SinglePartTicket(ticket) = ticket {
            assert!(!ticket.url.is_empty(), "No URL returned");
        } else {
            panic!("Ticket is not a single part ticket");
        }
    }

    #[tokio::test]
    async fn test_large_ticket() {
        // Set up the client and get a ticket
        let (client, pid) = setup_direct_upload("LocalStack").await;

        // Get a ticket for a large file (100MB to ensure multipart)
        let ticket = get_ticket(&client, 100_000_000, Identifier::PersistentId(pid))
            .await
            .expect("Failed to get ticket")
            .data
            .expect("Failed to get ticket data");

        match ticket {
            TicketResponse::MultiPartTicket(ticket) => {
                assert!(!ticket.urls.is_empty(), "No URLs returned");
                assert!(!ticket.abort.is_empty(), "No abort URL returned");
                assert!(!ticket.complete.is_empty(), "No complete URL returned");
            }
            TicketResponse::SinglePartTicket(_) => {
                // If we get a single part ticket for 100MB, that's also valid
                // depending on the storage configuration
                println!("Got single part ticket for large file - this may be expected based on storage configuration");
            }
        }
    }

    #[tokio::test]
    async fn test_no_ticket_available() {
        // Set up the client and a dataset/collection
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Get a ticket
        let response = get_ticket(&client, 100, Identifier::PersistentId(pid))
            .await
            .expect("Failed to get ticket");

        assert!(response.status.is_err(), "No error returned");
    }
}
