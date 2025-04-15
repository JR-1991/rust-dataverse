use crate::{
    client::{evaluate_response, BaseClient},
    prelude::Identifier,
    request::RequestType,
    response::Response,
};

use typify::import_types;

import_types!(schema = "models/dataset/review.json", struct_builder = true,);

/// Submits a dataset for review.
///
/// This asynchronous function sends a POST request to the API endpoint designated for submitting
/// a dataset for review. The function constructs the API endpoint URL dynamically, incorporating
/// the dataset's identifier.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset to be submitted for review.
///
/// # Returns
///
/// A `Result` wrapping a `Response<DatasetReviewResponse>`, which contains the HTTP response status
/// and the deserialized response data indicating the outcome of the submission operation,
/// or a `String` error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The API request fails.
/// - The response evaluation fails.
/// - The dataset is already under review.
pub async fn submit_for_review(
    client: &BaseClient,
    id: &Identifier,
) -> Result<Response<ReviewResponse>, String> {
    // Endpoint metadata
    let url = match id {
        Identifier::Id(id) => format!("api/datasets/{}/submitForReview", id),
        Identifier::PersistentId(pid) => format!(
            "api/datasets/:persistentId/submitForReview?persistentId={}",
            pid
        ),
    };

    // Send request
    let context = RequestType::Plain;
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<ReviewResponse>(response).await
}

/// Returns a dataset to the author.
///
/// This asynchronous function sends a POST request to the API endpoint designated for returning
/// a dataset to the author. The function constructs the API endpoint URL dynamically, incorporating
/// the dataset's identifier.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset to be returned from review.
/// * `reason_for_return` - A string slice representing the reason for returning the dataset to the author.
///
/// # Returns
///
/// A `Result` wrapping a `Response<ReviewResponse>`, which contains the HTTP response status
/// and the deserialized response data indicating the outcome of the return operation,
/// or a `String` error message on failure.
pub async fn return_to_author(
    client: &BaseClient,
    id: &Identifier,
    reason_for_return: &str,
) -> Result<Response<ReviewResponse>, String> {
    // Endpoint metadata
    let url = match id {
        Identifier::Id(id) => format!("api/datasets/{}/returnToAuthor", id),
        Identifier::PersistentId(pid) => format!(
            "api/datasets/:persistentId/returnToAuthor?persistentId={}",
            pid
        ),
    };

    // Prepare the body
    let body = serde_json::to_string(&ReturnToAuthorBody {
        reason_for_return: reason_for_return.to_string(),
    })
    .map_err(|e| format!("Failed to serialize return to author body: {}", e))?;

    // Send request
    let context = RequestType::JSON { body };
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<ReviewResponse>(response).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prelude::*,
        response::Status,
        test_utils::{create_test_dataset, extract_test_env},
    };

    #[tokio::test]
    async fn test_submit_for_review() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Submit for review
        let response = submit_for_review(&client, &Identifier::PersistentId(pid))
            .await
            .expect("Failed to submit for review");

        assert_eq!(response.status, Status::OK);
        let data = response.data.expect("No data returned");
        assert!(data.in_review);
        assert!(data.message.len() > 0);
    }

    #[tokio::test]
    async fn test_submit_for_review_with_id() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Submit for review
        let response = submit_for_review(&client, &Identifier::Id(id))
            .await
            .expect("Failed to submit for review");

        assert_eq!(response.status, Status::OK);
        let data = response.data.expect("No data returned");
        assert!(data.in_review);
        assert!(data.message.len() > 0);
    }

    #[tokio::test]
    async fn test_double_submit_fails() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Submit for review
        submit_for_review(&client, &Identifier::Id(id))
            .await
            .expect("Failed to submit for review");

        // Submit for review again
        let response = submit_for_review(&client, &Identifier::Id(id))
            .await
            .expect("Failed to submit for review");

        assert_eq!(response.status, Status::ERROR);
    }

    #[tokio::test]
    async fn test_return_to_author() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Submit for review
        submit_for_review(&client, &Identifier::Id(id))
            .await
            .expect("Failed to submit for review");

        // Return from review
        let response = return_to_author(&client, &Identifier::Id(id), "Test reason")
            .await
            .expect("Failed to return from review");

        assert_eq!(response.status, Status::OK);
        let data = response.data.expect("No data returned");
        assert!(!data.in_review);
        assert!(data.message.len() > 0);
    }
}
