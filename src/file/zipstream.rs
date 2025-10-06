use std::error::Error;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder, ZipString};
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use walkdir::WalkDir;

/// Sets up a recursive zip stream for a given directory path.
///
/// This function creates an mpsc channel and spawns a task to run the zip writer.
/// It returns a tuple containing the join handle for the spawned task and a receiver stream
/// for the zip data.
///
/// # Arguments
/// * `dir_path` - The path to the directory to be zipped.
///
/// # Returns
/// A tuple containing:
/// * A `JoinHandle` for the spawned task, which returns a `Result` indicating success or failure.
/// * A `ReceiverStream` that yields the zip data as `Vec<u8>`.
pub fn setup_recursive_zip_stream(
    dir_path: PathBuf,
) -> (
    tokio::task::JoinHandle<Result<(), String>>,
    ReceiverStream<Vec<u8>>,
) {
    let (tx, rx) = mpsc::channel(200);

    (
        tokio::spawn(run_zip_writer(dir_path, tx)),
        ReceiverStream::new(rx),
    )
}

/// A struct representing a stream that sends data to an mpsc channel.
#[derive(Debug, Clone)]
struct ZipStream {
    /// The sender part of the mpsc channel.
    sender: Sender<Vec<u8>>,
}

impl ZipStream {
    /// Creates a new `ZipStream` instance.
    ///
    /// # Arguments
    /// * `sender` - The sender part of the mpsc channel.
    ///
    /// # Returns
    /// A new `ZipStream` instance.
    pub(crate) fn new(sender: Sender<Vec<u8>>) -> Self {
        Self { sender }
    }

    /// Finishes the stream by dropping the sender.
    pub(crate) fn finish(self) {
        drop(self.sender);
    }
}

impl AsyncWrite for ZipStream {
    /// Polls to write data to the stream.
    ///
    /// # Arguments
    /// * `cx` - The context of the asynchronous task.
    /// * `buf` - The buffer containing the data to write.
    ///
    /// # Returns
    /// A `Poll` indicating whether the write operation is ready, pending, or resulted in an error.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let data = buf.to_vec();
        let sender = self.sender.clone();

        // Store the future inside the struct to track its progress between poll_write calls
        let send_fut = async move {
            sender
                .send(data)
                .await
                .map_err(io::Error::other)
        };

        // Pin the future
        let mut fut = Box::pin(send_fut);

        // Poll the future
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(buf.len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Polls to flush the stream.
    ///
    /// # Arguments
    /// * `_cx` - The context of the asynchronous task.
    ///
    /// # Returns
    /// A `Poll` indicating whether the flush operation is ready.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Polls to shut down the stream.
    ///
    /// # Arguments
    /// * `_cx` - The context of the asynchronous task.
    ///
    /// # Returns
    /// A `Poll` indicating whether the shutdown operation is ready.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Runs the zip writer task.
///
/// This function walks through the directory recursively and writes all files to the zip archive.
/// It sends the zip data through the provided mpsc channel.
///
/// # Arguments
/// * `dir_path` - The path to the directory to be zipped.
/// * `tx` - The sender part of the mpsc channel.
///
/// # Returns
/// A `Result` indicating success or failure.
pub(crate) async fn run_zip_writer(dir_path: PathBuf, tx: Sender<Vec<u8>>) -> Result<(), String> {
    let mut file = ZipStream::new(tx);
    let mut writer = ZipFileWriter::with_tokio(&mut file);
    let method = Compression::Deflate;

    // Walk recursively through a directory and write all files to the zip
    for entry in WalkDir::new(&dir_path).into_iter().filter_map(|e| e.ok()) {
        let is_dir = entry.file_type().is_dir();
        let has_hidden = contains_hidden(entry.path());

        if is_dir || has_hidden {
            continue;
        }

        stream_file_to_archive(&mut writer, entry.path().to_owned(), method, &dir_path)
            .await
            .map_err(|e| e.to_string())?;
    }

    writer.close().await.map_err(|e| e.to_string())?;

    file.finish();

    Ok(())
}

/// Streams a file to the zip archive.
///
/// This function reads the file data and writes it to the zip archive.
///
/// # Arguments
/// * `writer` - The zip file writer.
/// * `file_path` - The path to the file to be added to the zip archive.
/// * `method` - The compression method to be used.
///
/// # Returns
/// A `Result` indicating success or failure.
async fn stream_file_to_archive(
    writer: &mut ZipFileWriter<&mut ZipStream>,
    file_path: PathBuf,
    method: Compression,
    dir_path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let data = read_file(file_path.clone()).await?;

    // Remove the current pwd from the path
    let path: PathBuf = file_path.strip_prefix(dir_path).unwrap().to_owned();

    let zip_path = ZipString::from(path.to_string_lossy().as_ref());
    let builder = ZipEntryBuilder::new(zip_path, method);

    writer
        .write_entry_whole(builder, &data)
        .await
        .map_err(|e| e.into())
}

/// Reads a file and returns its data.
///
/// # Arguments
/// * `file_path` - The path to the file to be read.
///
/// # Returns
/// A `Result` containing the file data or an error.
async fn read_file(file_path: PathBuf) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = File::open(file_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    Ok(data)
}

/// Checks if a path contains hidden files or directories.
///
/// # Arguments
/// * `path` - The path to be checked.
///
/// # Returns
/// `true` if the path contains hidden files or directories, `false` otherwise.
fn contains_hidden(path: &Path) -> bool {
    path.components().any(|c| match c {
        std::path::Component::Normal(name) => name.to_string_lossy().starts_with('.'),
        _ => false,
    })
}
