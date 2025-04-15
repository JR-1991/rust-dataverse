#[cfg(test)]
mod tests {
    use std::fs;

    use lazy_static::lazy_static;

    use dataverse::client::BaseClient;
    use dataverse::identifier::Identifier;
    use dataverse::native_api::dataset;
    use dataverse::native_api::dataset::create::DatasetCreateBody;

    lazy_static! {
        static ref BASE_URL: String =
            std::env::var("BASE_URL").expect("BASE_URL must be set for tests");
    }

    lazy_static! {
        static ref API_TOKEN: String =
            std::env::var("API_TOKEN").expect("API_TOKEN must be set for tests");
    }

    #[tokio::test]
    async fn test_create_delete_dataset() {
        // Part 1: Create a dataset
        // Arrange
        let client = BaseClient::new(&BASE_URL, Some(&API_TOKEN)).unwrap();
        let body = fs::read_to_string("tests/fixtures/create_dataset_body.json")
            .expect("Could not read body");
        let body = serde_json::from_str::<DatasetCreateBody>(&body);

        // Act
        let response = dataset::create::create_dataset(&client, "Root", body.unwrap())
            .await
            .expect("Could not create dataset");

        // Assert
        assert_eq!(
            response.status,
            dataverse::response::Status::OK,
            "Create dataset failed"
        );

        // Part 2: Delete the dataset
        // Act
        let dataset_id = response.data.unwrap().id.expect("Could not get dataset id");
        let response = dataset::delete::delete_dataset(&client, &dataset_id)
            .await
            .expect("Could not delete dataset");

        // Assert
        assert_eq!(
            response.status,
            dataverse::response::Status::OK,
            "Delete dataset failed"
        );
    }

    #[tokio::test]
    async fn test_dataset_publish() {
        // Part 1: Create a dataset
        // Arrange
        let client = BaseClient::new(&BASE_URL, Some(&API_TOKEN)).unwrap();
        let body = fs::read_to_string("tests/fixtures/create_dataset_body.json")
            .expect("Could not read body");
        let body = serde_json::from_str::<DatasetCreateBody>(&body);

        // Act
        let response = dataset::create::create_dataset(&client, "Root", body.unwrap())
            .await
            .expect("Could not create dataset");

        // Assert
        assert_eq!(
            response.status,
            dataverse::response::Status::OK,
            "Create dataset failed"
        );

        // Part 2: Publish the dataset
        // Act
        let dataset_id = response
            .data
            .unwrap()
            .persistent_id
            .expect("Could not get dataset pid");
        let response = dataset::publish::publish_dataset(
            &client,
            &dataset_id,
            dataset::publish::Version::Major,
        )
        .await
        .expect("Could not publish dataset");

        // Assert
        assert_eq!(
            response.status,
            dataverse::response::Status::OK,
            "Publish dataset failed"
        );
    }

    #[tokio::test]
    async fn test_dataset_file_upload() {
        // Part 1: Create a dataset
        // Arrange
        let client = BaseClient::new(&BASE_URL, Some(&API_TOKEN)).unwrap();
        let body = fs::read_to_string("tests/fixtures/create_dataset_body.json")
            .expect("Could not read body");

        let body = serde_json::from_str::<DatasetCreateBody>(&body);
        let response = dataset::create::create_dataset(&client, "Root", body.unwrap())
            .await
            .expect("Could not create dataset");

        // Act
        let dataset_id = response
            .data
            .unwrap()
            .persistent_id
            .expect("Could not get dataset pid");
        let dataset_id = Identifier::PersistentId(dataset_id);
        let response = dataset::upload::upload_file_to_dataset(
            &client,
            dataset_id,
            "tests/fixtures/create_dataset_body.json",
            None,
            None,
        )
        .await
        .expect("Could not upload file");

        // Assert
        assert_eq!(
            response.status,
            dataverse::response::Status::OK,
            "Upload file failed"
        );
    }
}
