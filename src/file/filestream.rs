//! File streaming functionality for downloading and managing files
//!
//! This module provides functionality for:
//! - Streaming files from HTTP responses to disk
//! - Handling resumable downloads
//! - Creating necessary directories
//! - Opening files in appropriate modes
//! - Progress tracking during downloads

use std::error::Error;
use std::path::Path;

use colored::Colorize;
use futures::StreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::progress::setup_progress_log;

/// Streams the file bundle to the output directory.
///
/// This asynchronous function writes the file bundle to a zip file in the output directory.
///
/// # Arguments
///
/// * `out_dir` - A `Path` instance representing the output directory where the files will be saved.
/// * `file_name` - A `String` containing the name of the file bundle.
/// * `response` - A `Response` instance containing the file bundle.
/// * `file_size` - A `u64` representing the size of the file bundle.
/// * `downloaded_bytes` - An optional `u64` representing the number of bytes already downloaded.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(())`. On failure, it returns an error.
///
/// # Errors
///
/// This function will return an error if:
/// - The zip file cannot be created.
/// - The file bundle cannot be written to the zip file.
pub(crate) async fn stream_file(
    out_dir: &Path,
    file_name: String,
    response: reqwest::Response,
    file_size: u64,
    downloaded_bytes: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    // Prepare the output file
    let out_path = out_dir.join(file_name);
    create_dirs_from_path(&out_path).await?;
    let mut file = open_file(downloaded_bytes, &out_path).await?;

    // Stream the file bundle
    let mut stream = response.bytes_stream();
    let fname = out_path
        .file_name()
        .ok_or("Could not extract file name")?
        .to_str()
        .ok_or("Could not convert file name to string")?;

    let pb = setup_progress_log(file_size, downloaded_bytes, fname);

    while let Some(item) = stream.next().await {
        let chunk = item.unwrap();
        file.write_all(&chunk).await?;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message("Download complete");

    println!(
        "\n\nFile saved to: {}",
        out_path.to_str().unwrap().bold().green()
    );

    Ok(())
}

/// Creates all necessary parent directories for a given file path.
///
/// # Arguments
///
/// * `out_path` - A reference to a `Path` representing the file path.
///
/// # Returns
///
/// A `Result` indicating success or failure.
///
/// # Errors
///
/// This function will return an error if directory creation fails.
pub(crate) async fn create_dirs_from_path(out_path: &Path) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Opens a file for writing, resuming if necessary.
///
/// This asynchronous function opens a file for writing. If the file already exists and `downloaded_bytes` is provided,
/// it opens the file in append mode to resume writing.
///
/// # Arguments
///
/// * `downloaded_bytes` - An optional `u64` representing the number of bytes already downloaded.
/// * `out_path` - A reference to a `Path` representing the path to the file.
///
/// # Returns
///
/// A `Result` containing an open `File` instance on success.
///
/// # Errors
///
/// This function will return an error if the file cannot be opened.
async fn open_file(downloaded_bytes: Option<u64>, out_path: &Path) -> Result<File, Box<dyn Error>> {
    Ok(if out_path.exists() && downloaded_bytes.is_some() {
        println!(
            "\n{}: {}",
            "Resuming download".yellow().bold(),
            out_path.to_str().unwrap().bold().green()
        );
        OpenOptions::new().append(true).open(out_path).await?
    } else {
        File::create(out_path).await?
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use httpmock::MockServer;

    use crate::client::BaseClient;

    #[tokio::test]
    async fn test_open_new_file() {
        let path = Path::new("tests/fixtures/file.txt");
        let file = super::open_file(None, path).await.unwrap();
        assert!(file.metadata().await.is_ok());
    }

    #[tokio::test]
    async fn test_open_existing_file() {
        let path = Path::new("tests/fixtures/file.txt");
        let file = super::open_file(Some(10), path)
            .await
            .expect("Failed to open file");
        assert!(file.metadata().await.is_ok());
    }

    #[tokio::test]
    async fn test_stream_file() {
        // Arrange
        // Create a mock server
        let content = "Hello, world!";
        let expected_md5_hash = format!("{:x}", md5::compute(content));
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/file.txt");
            then.status(200).body(content);
        });

        // Create a temporary directory
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path();

        // Create a client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");
        let response = client
            .get("/file.txt", None, crate::request::RequestType::Plain, None)
            .await
            .expect("Failed to create request");

        // Act
        // Stream the file
        super::stream_file(out_dir, "file.txt".to_string(), response, 10, None)
            .await
            .expect("Failed to stream file");

        mock.assert();

        // Assert
        // Check that the file was created
        let file_path = out_dir.join("file.txt");
        assert!(file_path.exists());

        // Check that the file content is correct
        let file_content = tokio::fs::read_to_string(file_path).await.unwrap();
        assert_eq!(file_content, content);

        // Check that the file has the correct MD5 hash
        let file_md5_hash = md5::compute(file_content);
        assert_eq!(format!("{:x}", file_md5_hash), expected_md5_hash);
    }

    #[tokio::test]
    async fn test_stream_file_resumes() {
        // Arrange
        // Create a mock server
        let existing_content = "Hello,";
        let remaining_content = " world!";

        // Create a mock server
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/file.txt");
            then.status(200).body(remaining_content);
        });

        // Create a temporary directory
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path();

        // Create a client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");
        let response = client
            .get("/file.txt", None, crate::request::RequestType::Plain, None)
            .await
            .expect("Failed to create request");

        // Act
        // Stream the file
        super::stream_file(
            out_dir,
            "file.txt".to_string(),
            response,
            10,
            Some(existing_content.len() as u64),
        )
        .await
        .expect("Failed to stream file");

        mock.assert();

        // Assert
        // Check that the file was created
        let file_path = out_dir.join("file.txt");
        assert!(file_path.exists());

        // Check that the file content is correct
        let file_content = tokio::fs::read_to_string(file_path).await.unwrap();
        assert_eq!(file_content, remaining_content);
    }
}
