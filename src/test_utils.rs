use std::path::PathBuf;

use crate::native_api::admin::collection::storage::set_collection_storage_driver;
use crate::native_api::collection;
use crate::native_api::collection::create::{
    CollectionCreateBody, CollectionCreateBodyDataverseType,
};
use crate::native_api::dataset::edit::EditMetadataBody;
use crate::prelude::dataset::create::DatasetCreateBody;
use crate::prelude::dataset::upload::UploadBody;
use crate::prelude::{dataset, BaseClient};

/// Extracts environment variables required for test configurations.
///
/// This function retrieves the API token, the base URL of the Dataverse, and the Dataverse version
/// from the environment variables `API_TOKEN`, `BASE_URL`, and `DV_VERSION`, respectively.
/// These environment variables must be set prior to calling this function.
///
/// # Returns
/// A tuple containing the API token, Dataverse base URL, and Dataverse version as `String`s.
///
/// # Panics
/// This function will panic if any of the required environment variables are not set.
pub(crate) fn extract_test_env() -> (String, String, String) {
    let api_token = std::env::var("API_TOKEN").expect("API_TOKEN must be set");
    let dataverse_url = std::env::var("BASE_URL").expect("BASE_URL must be set");
    let dv_version = std::env::var("DV_VERSION").expect("DV_VERSION must be set");

    (api_token, dataverse_url, dv_version)
}

pub(crate) fn create_test_client() -> BaseClient {
    let (api_token, dataverse_url, _) = extract_test_env();
    BaseClient::new(&dataverse_url, Some(&api_token)).expect("Failed to create client")
}

pub(crate) fn prepare_dataset_body(path: PathBuf) -> DatasetCreateBody {
    let rdr = std::fs::File::open(path).expect("Failed to open dataset create body");
    serde_json::from_reader(rdr).expect("Failed to read dataset create body")
}

pub(crate) fn prepare_upload_body() -> UploadBody {
    let rdr = std::fs::File::open("tests/fixtures/upload_datafile_body.json")
        .expect("Failed to open upload body");
    serde_json::from_reader(rdr).expect("Failed to read upload body")
}

pub(crate) async fn create_test_dataset(client: &BaseClient, parent: &str) -> (i64, String) {
    let body = prepare_dataset_body("./tests/fixtures/create_dataset_body.json".into());
    let response = dataset::create::create_dataset(client, parent, body)
        .await
        .expect("Failed to create dataset");

    let data = response.data.expect("Failed to get dataset data");

    (data.id.unwrap(), data.persistent_id.unwrap())
}

pub(crate) fn prepare_test_collection() -> CollectionCreateBody {
    let name = format!("test_collection_{}", rand::random::<u16>());
    CollectionCreateBody {
        affiliation: "".to_string(),
        alias: name,
        dataverse_contacts: vec![collection::create::Contact {
            contact_email: "test@web.com".to_string(),
            display_order: None,
        }],
        name: "Some collection".into(),
        description: "This is a test collection".to_string(),
        dataverse_type: CollectionCreateBodyDataverseType::Department,
    }
}

pub(crate) fn prepare_edit_dataset_body() -> EditMetadataBody {
    let rdr = std::fs::File::open("tests/fixtures/edit_dataset_body.json")
        .expect("Failed to open edit dataset body");

    serde_json::from_reader(rdr).expect("Failed to read edit dataset body")
}

pub(crate) async fn create_test_collection(client: &BaseClient, parent: &str) -> String {
    let body = prepare_test_collection();
    let _ = collection::create::create_collection(client, parent, body.clone())
        .await
        .expect("Failed to create collection");

    body.alias
}

pub(crate) async fn set_storage_backend(client: &BaseClient, backend: &str, alias: &str) {
    set_collection_storage_driver(client, alias, backend)
        .await
        .expect("Failed to set storage backend");
}
