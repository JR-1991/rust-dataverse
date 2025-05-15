use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::{error::Error, io::SeekFrom};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use lazy_static::lazy_static;
use reqwest::{Body, Client, Url};
use tokio::io::AsyncReadExt;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeekExt},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{BytesCodec, FramedRead};
use variantly::Variantly;

use crate::prelude::CallbackFun;

lazy_static! {
    static ref BUFFER_SIZE: usize = std::env::var("DVCLI_BUFFER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2 * 1024 * 1024); // Default to 8GB if env var not set or invalid
}

/// A struct representing an upload file with a name, file source, and size.
///
/// This is the base struct that is used to perform uploads to a Dataverse dataset.
/// It can be used throughout all upload functions and provides a unified interface
/// for different file sources, such as local files, remote URLs, and genericstreams.
///
/// Please see the [`FileSource`] enum for more details on the different file sources.
#[derive(Debug)]
pub struct UploadFile {
    /// The name of the file.
    pub name: String,
    /// Directory path of the file.
    pub dir: Option<PathBuf>,
    /// The source of the file data, which can be a `ReceiverStream` or a `File`.
    ///
    /// Please see the [`FileSource`] enum for more details.
    pub file: FileSource,
    /// The size of the file in bytes.
    pub size: u64,
    /// The start position of the file in bytes.
    pub start: Option<u64>,
    /// The end position of the file in bytes.
    pub end: Option<u64>,
}

impl UploadFile {
    /// Creates a new `UploadFile` instance.
    ///
    /// # Arguments
    /// * `name` - The name of the file.
    /// * `file` - The source of the file data.
    /// * `size` - The size of the file in bytes.
    ///
    /// # Returns
    /// A new `UploadFile` instance.
    fn new(name: String, dir: Option<PathBuf>, file: FileSource, size: u64) -> Self {
        UploadFile {
            name,
            dir,
            file,
            size,
            start: None,
            end: None,
        }
    }

    /// Sets the start and end positions of the file.
    ///
    /// # Arguments
    /// * `start` - The start position of the file in bytes.
    /// * `end` - The end position of the file in bytes.
    pub(crate) fn set_bounds(&mut self, start: u64, end: u64) {
        self.start = Some(start);
        self.end = Some(end);
    }

    /// Creates a `Body` from the file source.
    ///
    /// # Arguments
    /// * `callback` - An optional callback function.
    /// * `pb` - A progress bar to track the upload progress.
    ///
    /// # Returns
    /// A `Result` containing the `Body` or an error.
    pub async fn create_body(
        self,
        callbacks: Option<Vec<CallbackFun>>,
        pb: ProgressBar,
    ) -> Result<Body, Box<dyn Error>> {
        match self.file {
            FileSource::ReceiverStream(stream) => {
                Self::create_receiver_stream(callbacks, pb, stream)
            }
            FileSource::File(mut file) => {
                if let (Some(start), Some(end)) = (self.start, self.end) {
                    file.seek(SeekFrom::Start(start)).await?;
                    let end_minus_start = end - start;
                    let limited_file = file.take(end_minus_start);
                    Self::create_file_stream(callbacks, pb, limited_file)
                } else {
                    Self::create_file_stream(callbacks, pb, file)
                }
            }
            FileSource::Path(path) => {
                let mut file = File::open(path).await?;
                if let (Some(start), Some(end)) = (self.start, self.end) {
                    file.seek(SeekFrom::Start(start)).await?;
                    let end_minus_start = end - start;
                    let limited_file = file.take(end_minus_start);
                    Self::create_file_stream(callbacks, pb, limited_file)
                } else {
                    Self::create_file_stream(callbacks, pb, file)
                }
            }
            FileSource::RemoteUrl(url) => Self::create_remote_stream(callbacks, pb, url).await,
        }
    }

    /// Creates multiple `UploadFile`s from the file source.
    ///
    /// Important, this method can only be used with File/Paths
    /// since the size of an URL or ReceiverStream is not known
    /// prior to the upload.
    ///
    /// # Arguments
    /// * `part_size` - The size of each chunk in bytes.
    ///
    /// # Returns
    /// A `Result` containing a vector of `UploadFile`s or an error.
    pub async fn chunk_file(self, part_size: u64) -> Result<Vec<UploadFile>, Box<dyn Error>> {
        if self.file.is_receiver_stream() || self.file.is_remote_url() {
            return Err("This method can only be used with File/Paths".into());
        }

        let mut chunked_files = vec![];
        let mut bytes_chunked = 0_u64;

        // Process file in chunks until we've processed the entire file
        while bytes_chunked < self.size {
            let start = bytes_chunked;
            let end = (bytes_chunked + part_size).min(self.size);

            // Create a new upload file with the current chunk bounds
            let mut upload_file = clone_upload_file(&self).await?;
            upload_file.set_bounds(start, end);

            chunked_files.push(upload_file);

            bytes_chunked = end;
        }

        Ok(chunked_files)
    }

    /// Creates a `Body` from a `ReceiverStream`.
    ///
    /// # Arguments
    /// * `callback` - An optional callback function.
    /// * `pb` - A progress bar to track the upload progress.
    /// * `stream` - The `ReceiverStream` to be uploaded.
    ///
    /// # Returns
    /// A `Result` containing the `Body` or an error.
    fn create_receiver_stream(
        callbacks: Option<Vec<CallbackFun>>,
        pb: ProgressBar,
        stream: ReceiverStream<Vec<u8>>,
    ) -> Result<Body, Box<dyn Error>> {
        Ok(Body::wrap_stream(
            stream
                .map(|chunk| Ok::<_, io::Error>(Bytes::from(chunk)))
                .inspect_ok(move |chunk| {
                    if let Some(callbacks) = &callbacks {
                        for callback in callbacks {
                            callback.call(chunk);
                        }
                    }
                    pb.inc(chunk.len() as u64);
                }),
        ))
    }

    /// Creates a `Body` from a `File`.
    ///
    /// # Arguments
    /// * `callback` - An optional callback function.
    /// * `pb` - A progress bar to track the upload progress.
    /// * `file` - The file to be uploaded.
    ///
    /// # Returns
    /// A `Result` containing the `Body` or an error.
    fn create_file_stream<T>(
        callbacks: Option<Vec<CallbackFun>>,
        pb: ProgressBar,
        file: T,
    ) -> Result<Body, Box<dyn Error>>
    where
        T: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let stream = FramedRead::with_capacity(file, BytesCodec::new(), *BUFFER_SIZE)
            .map_ok(bytes::Bytes::from)
            .inspect_ok(move |chunk| {
                if let Some(callbacks) = &callbacks {
                    for callback in callbacks {
                        callback.call(chunk);
                    }
                }
                pb.inc(chunk.len() as u64);
            });

        Ok(Body::wrap_stream(stream))
    }

    /// Creates a `Body` from a remote URL.
    ///
    /// # Arguments
    /// * `callback` - An optional callback function.
    /// * `pb` - A progress bar to track the upload progress.
    /// * `url` - The URL to download the file from.
    ///
    /// # Returns
    /// A `Result` containing the `Body` or an error.
    async fn create_remote_stream(
        callbacks: Option<Vec<CallbackFun>>,
        pb: ProgressBar,
        url: Url,
    ) -> Result<Body, Box<dyn Error>> {
        let client = Client::new();
        let response = client.get(url.clone()).send().await?;

        // Check if the request was successful
        if !response.status().is_success() {
            return Err(format!("Failed to download from {}: {:?}", url, response.status()).into());
        }

        // Stream the response body
        let response_stream = response.bytes_stream().inspect_ok(move |chunk| {
            if let Some(callbacks) = &callbacks {
                for callback in callbacks {
                    callback.call(chunk);
                }
            }

            pb.inc(chunk.len() as u64);
        });

        Ok(Body::wrap_stream(response_stream))
    }

    /// Creates an `UploadFile` instance from a file path.
    ///
    /// # Arguments
    /// * `path` - The path to the file.
    ///
    /// # Returns
    /// A `Result` containing the `UploadFile` instance or an error.
    pub async fn from_path(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let name = path
            .file_name()
            .ok_or("Could not extract file name")?
            .to_str()
            .ok_or("Could not convert file name to string")?;
        let file = File::open(path.clone()).await.unwrap();
        let dir = path.parent().map(|p| p.to_path_buf());
        let size = file.metadata().await?.len();

        Ok(UploadFile::new(name.to_string(), dir, file.into(), size))
    }

    /// Get the size of the file.
    ///
    /// # Returns
    /// A `Result` containing the size of the file or an error.
    pub(crate) async fn get_size(&self) -> Result<u64, Box<dyn Error>> {
        if let (Some(start), Some(end)) = (self.start, self.end) {
            Ok(end - start)
        } else {
            match &self.file {
                FileSource::File(file) => Ok(file.metadata().await?.len()),
                FileSource::Path(path) => Ok(std::fs::metadata(path).unwrap().len()),
                FileSource::RemoteUrl(_) => Ok(0),
                FileSource::ReceiverStream(_) => Ok(0),
            }
        }
    }
}

/// Clones an `UploadFile` instance.
///
/// This method is used to clone an `UploadFile` instance. It is used to create
/// a new `UploadFile` instance from an existing one. Please note that this
/// method is only supported for `File` and `Path` variants and used within
/// the multipart upload process.
///
/// # Arguments
/// * `upload_file` - The `UploadFile` instance to clone.
///
/// # Returns
/// A `Result` containing the cloned `UploadFile` instance or an error.
pub(crate) async fn clone_upload_file(
    upload_file: &UploadFile,
) -> Result<UploadFile, Box<dyn Error>> {
    match &upload_file.file {
        FileSource::File(file) => Ok(UploadFile::new(
            upload_file.name.clone(),
            upload_file.dir.clone(),
            FileSource::File(file.try_clone().await.map_err(|_| "Could not clone file")?),
            upload_file.size,
        )),
        FileSource::Path(path) => Ok(UploadFile::new(
            upload_file.name.clone(),
            upload_file.dir.clone(),
            FileSource::Path(path.clone()),
            upload_file.size,
        )),
        _ => Err("Cloning is only supported for File/Paths".into()),
    }
}

impl From<&str> for UploadFile {
    /// Converts a `&str` into an `UploadFile` instance.
    ///
    /// # Arguments
    /// * `path` - The path to the file.
    ///
    /// # Returns
    /// A new `UploadFile` instance.
    fn from(path: &str) -> Self {
        let source = match Url::parse(path) {
            Ok(url) => FileSource::RemoteUrl(url),
            Err(_) => FileSource::Path(relative_to_absolute_path(&PathBuf::from(path)).unwrap()),
        };

        let fsize = match source {
            FileSource::RemoteUrl(_) => 0,
            _ => std::fs::metadata(path)
                .expect("Could not get file metadata")
                .len(),
        };

        let fname = match &source {
            FileSource::RemoteUrl(_) => "file".to_string(),
            FileSource::Path(path) => PathBuf::from(path)
                .file_name()
                .expect("Could not get file name")
                .to_str()
                .expect("Could not convert file name to string")
                .to_string(),
            _ => panic!("Expected RemoteUrl or Path variant"),
        };

        Self::new(fname, None, source, fsize)
    }
}

impl FromStr for UploadFile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(UploadFile::from(s))
    }
}

impl From<PathBuf> for UploadFile {
    /// Converts a `PathBuf` into an `UploadFile` instance.
    ///
    /// # Arguments
    /// * `path` - The path to the file.
    ///
    /// # Returns
    /// A new `UploadFile` instance.
    fn from(path: PathBuf) -> Self {
        assert!(path.exists(), "File does not exist");

        // Convert the path to an absolute path, if it is relative
        let absolute_path = relative_to_absolute_path(&path).unwrap();
        let fsize = std::fs::metadata(&absolute_path)
            .expect("Could not get file metadata")
            .len();

        let fname = path
            .file_name()
            .expect("Could not get file name")
            .to_str()
            .expect("Could not convert file name to string")
            .to_string();

        let dir = path.parent().map(|p| p.to_path_buf());

        Self::new(fname, dir, FileSource::Path(absolute_path), fsize)
    }
}

impl From<Url> for UploadFile {
    /// Converts a `Url` into an `UploadFile` instance.
    ///
    /// # Arguments
    /// * `url` - The URL to download the file from.
    ///
    /// # Returns
    /// A new `UploadFile` instance.
    fn from(url: Url) -> Self {
        Self::new(url.to_string(), None, FileSource::RemoteUrl(url), 0)
    }
}

impl From<ReceiverStream<Vec<u8>>> for UploadFile {
    /// Converts a `ReceiverStream` into an `UploadFile` instance.
    ///
    /// # Arguments
    /// * `stream` - The `ReceiverStream` to convert.
    ///
    /// # Returns
    /// A new `UploadFile` instance.
    fn from(stream: ReceiverStream<Vec<u8>>) -> Self {
        Self::new("stream.zip".to_string(), None, stream.into(), 0)
    }
}

/// An enum representing a source of file data, which can either be a `ReceiverStream`,
/// a `File`, a `PathBuf`, or a `Url`.
///
/// This is very powerful for many cases of file uploads:
///
/// - Streaming within the same process
/// - Streaming a file from a remote URL
/// - Streaming a file from a local file system
///
/// While the "classic" cases such as local and remote files are supported,
/// the ReceiverStream variant is the most flexible, as data can be streamed
/// from any source.
#[derive(Debug, Variantly)]
pub enum FileSource {
    /// A `ReceiverStream` of file data.
    ///
    /// This type of file source is  used to stream within the same process.
    /// For example, if you may run some other process such as a simulation or
    /// zip writer and you want to upload the output live to a dataset, you can use this
    /// type of file source.
    ReceiverStream(ReceiverStream<Vec<u8>>),

    /// A `File` of file data.
    ///
    /// This type of file source is used to upload a file from an
    /// async file handler such as `tokio::fs::File::open`.
    File(File),

    /// A `PathBuf` of file data.
    ///
    /// This type of file source is used to upload a file from a local filesystem.
    Path(PathBuf),

    /// A `Url` of file data.
    ///
    /// This type of file source is used to upload a file from a remote URL. The data
    /// is "tunneled" through a buffered stream to avoid downloading the entire file
    /// into memory.
    RemoteUrl(Url),
}

impl FileSource {
    /// Returns the absolute path of the file source.   
    ///
    /// # Returns
    /// A `Result` containing the absolute path of the file source or an error.
    pub fn absolute_path(&self) -> Option<PathBuf> {
        match self {
            FileSource::Path(path) => Some(relative_to_absolute_path(path).unwrap()),
            _ => None,
        }
    }
}

impl From<ReceiverStream<Vec<u8>>> for FileSource {
    /// Converts a `ReceiverStream` into a `FileSource`.
    ///
    /// # Arguments
    /// * `stream` - The `ReceiverStream` to convert.
    ///
    /// # Returns
    /// A `FileSource` containing the provided `ReceiverStream`.
    fn from(stream: ReceiverStream<Vec<u8>>) -> Self {
        FileSource::ReceiverStream(stream)
    }
}

// This implementation will use the default `T = u8` if no type is specified.
impl From<File> for FileSource {
    /// Converts a `File` into a `FileSource` with default type `u8`.
    ///
    /// # Arguments
    /// * `file` - The `File` to convert.
    ///
    /// # Returns
    /// A `FileSource` containing the provided `File`.
    fn from(file: File) -> Self {
        FileSource::File(file)
    }
}

impl From<&str> for FileSource {
    /// Converts a `&str` into a `FileSource` with default type `u8`.
    ///
    /// # Arguments
    /// * `path` - The path to the file.
    ///
    /// # Returns
    /// A `FileSource` containing the provided file path.
    fn from(path: &str) -> Self {
        // Check if it is a remote URL
        if let Ok(url) = Url::parse(path) {
            FileSource::RemoteUrl(url)
        } else {
            let path = PathBuf::from(path);
            assert!(path.exists(), "File does not exist");

            // Convert the path to an absolute path, if it is relative
            let absolute_path = relative_to_absolute_path(&path).unwrap();

            FileSource::Path(absolute_path)
        }
    }
}

impl From<String> for FileSource {
    /// Converts a `String` into a `FileSource` with default type `u8`.
    ///
    /// # Arguments
    /// * `path` - The path to the file.
    ///
    /// # Returns
    /// A `FileSource` containing the provided file path.
    fn from(path: String) -> Self {
        // Check if it is a remote URL
        if let Ok(url) = Url::parse(&path) {
            FileSource::RemoteUrl(url)
        } else {
            let path = PathBuf::from(path);
            assert!(path.exists(), "File does not exist");

            // Convert the path to an absolute path, if it is relative
            let absolute_path = relative_to_absolute_path(&path).unwrap();

            FileSource::Path(absolute_path)
        }
    }
}

impl From<Url> for FileSource {
    /// Converts a `Url` into a `FileSource`.
    ///
    /// # Arguments
    /// * `url` - The `Url` to convert.
    ///
    /// # Returns
    /// A `FileSource` containing the provided `Url`.
    fn from(url: Url) -> Self {
        FileSource::RemoteUrl(url)
    }
}

impl FromStr for FileSource {
    type Err = String;

    /// Converts a string slice to a `FileSource`.
    ///
    /// # Arguments
    /// * `s` - A string slice that holds the file source information.
    ///
    /// # Returns
    /// A `Result` which is:
    /// - `Ok(FileSource)` if the string matches one of the predefined sources.
    /// - `Err(String)` if an error occurs.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Url::parse(s) {
            Ok(url) => Ok(FileSource::RemoteUrl(url)),
            Err(_) => {
                let path = PathBuf::from(s);
                if path.exists() {
                    Ok(FileSource::Path(relative_to_absolute_path(&path).unwrap()))
                } else {
                    Err("Invalid file source".to_string())
                }
            }
        }
    }
}

/// Converts a relative path to an absolute path.
///
/// # Arguments
/// * `path` - The path to convert.
///
/// # Returns
/// A `Result` containing the absolute path or an error.
pub(crate) fn relative_to_absolute_path(path: &PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute() {
        Ok(path.clone())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use httpmock::MockServer;
    use tokio::fs::File;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::client::BaseClient;
    use crate::request::RequestType;

    use super::*;

    /// Test converting a `&str` to a `UploadFile`.
    #[tokio::test]
    async fn convert_string_slice_to_upload_file() {
        let path = "tests/fixtures/file.txt";
        let size = std::fs::metadata(path)
            .expect("Could not get file metadata")
            .len();
        let upload_file = UploadFile::from(path);

        match upload_file.file {
            FileSource::Path(path) => {
                assert_eq!(path, path);
            }
            _ => panic!("Expected Path variant"),
        }

        assert_eq!(upload_file.size, size);
    }

    /// Test converting a `PathBuf` to a `UploadFile`.
    #[tokio::test]
    async fn convert_path_buf_to_upload_file() {
        let path = PathBuf::from("tests/fixtures/file.txt");
        let size = std::fs::metadata(&path)
            .expect("Could not get file metadata")
            .len();
        let upload_file = UploadFile::from(path);

        match upload_file.file {
            FileSource::Path(path) => {
                assert_eq!(path, path);
            }
            _ => panic!("Expected Path variant"),
        }

        assert_eq!(upload_file.size, size);
    }

    /// Test converting a `Url` to a `UploadFile`.
    #[tokio::test]
    async fn convert_url_to_upload_file() {
        let url = Url::parse("https://example.com/file.txt").unwrap();
        let upload_file = UploadFile::from(url);

        match upload_file.file {
            FileSource::RemoteUrl(url) => {
                assert_eq!(url, url);
            }
            _ => panic!("Expected RemoteUrl variant"),
        }

        assert_eq!(upload_file.size, 0);
    }

    /// Test converting a `ReceiverStream` to a `UploadFile`.
    #[tokio::test]
    async fn convert_receiver_stream_to_upload_file() {
        let (_, rx) = mpsc::channel(10);
        let stream = ReceiverStream::new(rx);
        let upload_file = UploadFile::from(stream);

        match upload_file.file {
            FileSource::ReceiverStream(_) => {}
            _ => panic!("Expected ReceiverStream variant"),
        }

        assert_eq!(upload_file.size, 0);
    }

    /// Test converting a `ReceiverStream` to a `FileSource`.
    #[tokio::test]
    async fn converts_receiver_stream_to_file_source() {
        let (_, rx) = mpsc::channel(10);
        let stream = ReceiverStream::new(rx);
        let file_source = FileSource::from(stream);

        match file_source {
            FileSource::ReceiverStream(_) => {}
            _ => panic!("Expected ReceiverStream variant"),
        }
    }

    /// Test converting a `File` to a `FileSource`.
    #[tokio::test]
    async fn converts_file_to_file_source() {
        let file = File::open("tests/fixtures/file.txt").await.unwrap();
        let file_source = FileSource::from(file);

        match file_source {
            FileSource::File(_) => {}
            _ => panic!("Expected File variant"),
        }
    }

    /// Test converting a `&str` to a `FileSource`.
    #[tokio::test]
    async fn convert_string_slice_to_file_source() {
        let path = "tests/fixtures/file.txt";
        let file_source = FileSource::from(path);

        match file_source {
            FileSource::Path(path) => {
                assert_eq!(path, path);
            }
            _ => panic!("Expected Path variant"),
        }
    }

    /// Test converting a `String` to a `FileSource`.
    #[tokio::test]
    async fn convert_string_to_file_source() {
        let path = "tests/fixtures/file.txt";
        let file_source = FileSource::from(path.to_string());

        match file_source {
            FileSource::Path(path) => {
                assert_eq!(path, path);
            }
            _ => panic!("Expected Path variant"),
        }
    }

    /// Test converting a `Url` to a `FileSource`.
    #[tokio::test]
    async fn convert_url_to_file_source() {
        let url = "https://example.com/file.txt";
        let file_source = FileSource::from(url);

        match file_source {
            FileSource::RemoteUrl(url) => {
                assert_eq!(url, url);
            }
            _ => panic!("Expected RemoteUrl variant"),
        }
    }

    /// Test converting a `ReceiverStream` with a different type to a `FileSource`.
    #[tokio::test]
    async fn receiver_stream_with_different_type() {
        let (_, rx) = mpsc::channel(10);
        let stream = ReceiverStream::new(rx);
        let file_source = FileSource::from(stream);

        match file_source {
            FileSource::ReceiverStream(_) => {}
            _ => panic!("Expected ReceiverStream variant"),
        }
    }

    #[tokio::test]
    async fn test_limited_file_stream() {
        // Create a mock server
        let path = "tests/fixtures/upload.txt";

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::PUT).path("/upload").body('H');
            then.status(200);
        });

        // Act
        let mut file: UploadFile = path.into();
        file.set_bounds(0, 1);

        // Setup the client and context
        let context = RequestType::File {
            file,
            callbacks: None,
        };
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");

        // Execute the request
        client
            .put("/upload", None, context, None)
            .await
            .expect("Could not execute request");

        // Assert
        mock.assert(); // Check if the mock was called
    }

    /// Tests transfer of a receiver stream to a mock server.
    ///
    /// This tests runs the following steps:
    /// 1. Create a mock server
    /// 2. Create a channel and spawn a continuous writer task
    /// 3. Create a receiver stream from the channel
    /// 4. Create an upload file from the receiver stream
    /// 5. Send the upload file to the mock server
    /// 6. Wait for the writer task to complete
    /// 7. Assert that the mock server was called
    ///
    #[tokio::test]
    async fn test_receiver_stream_upload() {
        let content = "Hello, world!";
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::PUT)
                .path("/upload")
                .body(content);
            then.status(200);
        });

        // Setup the client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");

        // Create a channel and spawn a continuous writer task
        let (tx, rx) = mpsc::channel(100);
        let writer_task = tokio::spawn(async move {
            let sender = tx;
            for chunk in content.as_bytes().chunks(4) {
                sender
                    .send(chunk.to_vec())
                    .await
                    .expect("Could not send chunk");
            }
            Ok::<_, String>(())
        });

        // Create upload file from receiver
        let stream = ReceiverStream::new(rx);
        let mut upload_file = UploadFile::from(stream);
        upload_file.size = 100;

        let context = RequestType::File {
            file: upload_file,
            callbacks: None,
        };

        // Send request
        client.put("/upload", None, context, None).await.unwrap();

        // Wait for writer to complete
        writer_task
            .await
            .expect("Writer task failed")
            .expect("Sender task failed");

        mock.assert();
    }

    /// Tests that a file can be uploaded to a mock server and downloaded from the same mock server.
    ///
    /// This tests runs the following steps:
    /// 1. Create a mock server
    /// 2. Create a mock for the download endpoint
    /// 3. Create a mock for the upload endpoint
    /// 4. Setup the client
    /// 5. Upload the file to the mock server
    /// 6. Download the file from the mock server
    /// 7. Assert that the upload and download mocks were called
    #[tokio::test]
    async fn test_url_upload() {
        let content = "Hello, world!";
        let server = MockServer::start();
        let download_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/download");
            then.status(200).body(content);
        });

        let upload_mock = server.mock(|when, then| {
            when.method(httpmock::Method::PUT)
                .path("/upload")
                .body(content);
            then.status(200);
        });

        // Setup the client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");

        // Download the content
        let remote_url = Url::parse(&server.url("/download")).unwrap();
        let upload_file = UploadFile::from(remote_url);
        let context = RequestType::File {
            file: upload_file,
            callbacks: None,
        };

        // Upload the content
        client
            .put("/upload", None, context, None)
            .await
            .expect("Could not upload");

        // Assert that the upload was successful
        upload_mock.assert();
        download_mock.assert();
    }

    /// Tests that file chunking works correctly by:
    /// 1. Creating a temporary file with known content
    /// 2. Chunking the file into parts of a specific size
    /// 3. Setting up mock HTTP endpoints for each chunk
    /// 4. Uploading each chunk to its corresponding endpoint
    /// 5. Verifying that:
    ///    - The correct number of chunks was created
    ///    - Each chunk contains the expected content
    ///    - Each chunk was uploaded to the correct endpoint
    #[tokio::test]
    async fn test_sent_chunks_by_path() {
        // Arrange: Set up test environment
        let part_size = 1024; // Each chunk will be 1KB

        // Setup the mock server to receive our uploads
        let server = MockServer::start();
        let mut mocks: Vec<httpmock::Mock> = vec![];

        // Create a temporary file with predictable content
        let mut file = tempfile::NamedTempFile::new().expect("Could not create temp file");

        // Generate content with 997 numbers concatenated together
        let content = (1..=997)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("");

        file.write_all(content.as_bytes())
            .expect("Could not write to file");

        // Convert the file to our UploadFile type
        let file: UploadFile = file.path().to_path_buf().into();

        // Create a mock endpoint for each expected chunk
        // This allows us to verify both the content and number of chunks
        let num_chunks = content.len().div_ceil(part_size);
        for i in 0..num_chunks {
            // Calculate the expected content for this chunk
            let part = {
                let start = i * part_size;
                let end = std::cmp::min((i + 1) * part_size, content.len());
                content[start..end].to_string()
            };

            // Create a mock that expects this specific chunk content
            let mock = server.mock(|when, then| {
                when.method(httpmock::Method::PUT)
                    .path(format!("/upload/{}", i).as_str())
                    .header("Content-Length", part.len().to_string())
                    .body(part); // Verify exact content of each chunk
                then.status(200);
            });

            mocks.push(mock);
        }

        // Setup the HTTP client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");

        // Act: Chunk the file and upload each part
        let chunked_files = file
            .chunk_file(part_size as u64)
            .await
            .expect("Could not create chunked bodies");
        let n_files = chunked_files.len();

        // Upload each chunk to its corresponding endpoint
        for (i, file) in chunked_files.into_iter().enumerate() {
            let context = RequestType::File {
                file,
                callbacks: None,
            };

            client
                .put(format!("/upload/{}", i).as_str(), None, context, None)
                .await
                .expect("Could not send request");
        }

        // Assert: Verify test expectations
        assert_eq!(n_files, 3, "Expected exactly 3 chunks for our test file");

        // Verify that each mock was called exactly once with the correct content
        for mock in mocks.into_iter() {
            mock.assert();
        }
    }

    #[tokio::test]
    async fn test_sent_chunks_by_file() {
        // Arrange: Set up test environment
        let part_size = 1024; // Each chunk will be 1KB

        // Setup the mock server to receive our uploads
        let server = MockServer::start();
        let mut mocks: Vec<httpmock::Mock> = vec![];

        // Create a temporary file with predictable content
        let mut temp_file = tempfile::NamedTempFile::new().expect("Could not create temp file");

        // Generate content with 997 numbers concatenated together
        let content = (1..=997)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("");

        temp_file
            .write_all(content.as_bytes())
            .expect("Could not write to file");

        // Convert the file to our UploadFile type
        let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let file = UploadFile::new(
            temp_file.path().to_str().unwrap().to_string(),
            None,
            FileSource::File(file),
            file_size,
        );

        // Create a mock endpoint for each expected chunk
        // This allows us to verify both the content and number of chunks
        let num_chunks = content.len().div_ceil(part_size);
        for i in 0..num_chunks {
            // Calculate the expected content for this chunk
            let part = {
                let start = i * part_size;
                let end = std::cmp::min((i + 1) * part_size, content.len());
                content[start..end].to_string()
            };

            // Create a mock that expects this specific chunk content
            let mock = server.mock(|when, then| {
                when.method(httpmock::Method::PUT)
                    .path(format!("/upload/{}", i).as_str())
                    .header("Content-Length", part.len().to_string())
                    .body(part); // Verify exact content of each chunk
                then.status(200);
            });

            mocks.push(mock);
        }

        // Setup the HTTP client
        let client = BaseClient::new(&server.url("/"), None).expect("Could not create client");

        // Act: Chunk the file and upload each part
        let chunked_files = file
            .chunk_file(part_size as u64)
            .await
            .expect("Could not create chunked bodies");
        let n_files = chunked_files.len();

        // Upload each chunk to its corresponding endpoint
        for (i, file) in chunked_files.into_iter().enumerate() {
            let context = RequestType::File {
                file,
                callbacks: None,
            };

            client
                .put(format!("/upload/{}", i).as_str(), None, context, None)
                .await
                .expect("Could not send request");
        }

        // Assert: Verify test expectations
        assert_eq!(n_files, 3, "Expected exactly 3 chunks for our test file");

        // Verify that each mock was called exactly once with the correct content
        for mock in mocks.into_iter() {
            mock.assert();
        }
    }

    #[test]
    fn test_relative_to_absolute_path() {
        let path = PathBuf::from("test.txt");
        let current_dir = std::env::current_dir().unwrap();
        let absolute_path = relative_to_absolute_path(&path).unwrap();
        assert_eq!(absolute_path, current_dir.join("test.txt"));
    }

    #[test]
    fn test_absolute_path() {
        let path = std::env::current_dir()
            .unwrap()
            .join(PathBuf::from("/test.txt"));
        let absolute_path = relative_to_absolute_path(&path).unwrap();
        assert_eq!(absolute_path, path);
    }
}
