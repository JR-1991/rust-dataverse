//! This module contains the models for the database used
//! to store the uploads and parts of a Dataverse S3 multi-part upload.
//!
//! ## Database Schema
//!
//! - The hash is calculated from the base_url, path, pid and size of the upload.
//! - The parts are inserted into the parts table with the hash of the upload as the foreign key.
//! - The parts are deleted when the upload is deleted.
//!
//! ## Uploads Table
//!
//! | Column Name  | Type    | Description |
//! |--------------|---------|-------------|
//! | hash         | TEXT    | Unique hash identifier for the upload |
//! | storage_id   | TEXT    | Storage ID of the upload |
//! | base_url     | TEXT    | Base Dataverse URL |
//! | pid          | TEXT    | Process ID associated with the upload |
//! | path         | TEXT    | File path of the upload |
//! | is_complete  | BOOLEAN | Flag indicating whether the upload is complete |
//! | complete_url | TEXT    | URL to call when completing the upload |
//! | abort_url    | TEXT    | URL to call when aborting the upload |
//! | size         | INTEGER | Size of the upload in bytes |
//! | part_size    | INTEGER | Part size in bytes |
//! | file_hash    | TEXT    | MD5 hash of the file |
//! | file_hash_algo    | TEXT    | Algorithm used to hash the file |
//!
//! ## Parts Table
//!
//! | Column Name  | Type    | Description |
//! |--------------|---------|-------------|
//! | id           | INTEGER | Unique identifier for the part |
//! | hash         | TEXT    | Hash identifier linking to the parent upload |
//! | start_byte   | INTEGER | Start byte of the part |
//! | end_byte     | INTEGER | End byte of the part |
//! | url          | TEXT    | URL of the part |
//! | expires_at   | TEXT    | Expiration timestamp for the part |
//! | etag         | TEXT    | ETag (entity tag) for the part |
//! | part_number  | TEXT    | Part number within the multi-part upload |
//! | is_uploaded  | BOOLEAN | Flag indicating whether the part is uploaded |
//! | file_path    | TEXT    | File path for the part |

use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{Duration, NaiveDateTime, Utc};
use reqwest::Url;
use sqlx::{prelude::FromRow, sqlite::SqliteQueryResult};

use crate::{
    client::BaseClient,
    file::{uploadfile::FileSource, UploadFile},
    prelude::{CallbackFun, Identifier},
    request::RequestType,
};

use super::{ticket::MultiPartTicket, upload::TestMode};

/// Represents an upload in the database
///
/// An upload contains information about a file being uploaded,
/// including its path, completion status, and URLs for completing
/// or aborting the upload process.
#[derive(Clone, FromRow, Debug)]
pub struct Upload {
    /// Base Dataverse URL
    pub base_url: String,
    /// Storage ID of the upload
    pub storage_id: String,
    /// Process ID associated with the upload
    pub pid: String,
    /// File path of the upload
    pub path: String,
    /// Unique hash identifier for the upload
    pub hash: String,
    /// Flag indicating whether the upload is complete
    pub is_complete: bool,
    /// URL to call when completing the upload
    pub complete_url: String,
    /// URL to call when aborting the upload
    pub abort_url: String,
    /// Size of the upload in bytes
    pub size: i64,
    /// Part size in bytes
    pub part_size: i64,
    /// MD5 hash of the file
    pub file_hash: Option<String>,
    /// Algorithm used to hash the file
    pub file_hash_algo: String,
}

impl Upload {
    /// SQL statement to create the uploads table if it doesn't exist
    const CREATE_UPLOADS_TABLE: &str = "CREATE TABLE IF NOT EXISTS uploads (
        hash TEXT NOT NULL PRIMARY KEY,
        storage_id TEXT NOT NULL,
        base_url TEXT NOT NULL,
        pid TEXT NOT NULL,
        path TEXT NOT NULL,
        is_complete BOOLEAN NOT NULL,
        complete_url TEXT NOT NULL,
        abort_url TEXT NOT NULL,
        size INTEGER NOT NULL,
        part_size INTEGER NOT NULL,
        file_hash TEXT NULL,
        file_hash_algo TEXT NULL
    )";

    /// SQL statement to insert a new upload into the database
    const INSERT_UPLOADS_TABLE: &str = "INSERT INTO uploads (hash, storage_id, base_url, pid, path, is_complete, complete_url, abort_url, size, part_size, file_hash, file_hash_algo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// SQL statement to select all uploads from the database
    const SELECT_UPLOADS_TABLE: &str = "SELECT * FROM uploads";

    /// SQL statement to select an upload by its hash identifier
    const SELECT_UPLOADS_TABLE_BY_HASH: &str = "SELECT * FROM uploads WHERE hash = ?";

    /// SQL statement to delete an upload by its hash identifier
    const DELETE_UPLOADS_TABLE_BY_HASH: &str = "DELETE FROM uploads WHERE hash = ?";

    /// SQL statement to select all parts by their hash identifier, by using a JOIN
    /// with the uploads table
    const SELECT_PARTS_TABLE_BY_HASH: &str =
        "SELECT * FROM parts JOIN uploads ON parts.hash = uploads.hash WHERE uploads.hash = ?";

    /// SQL statement to select all parts by their hash identifier, by using a JOIN
    /// with the uploads table
    const SELECT_PARTS_TABLE_BY_HASH_AND_INCOMPLETE: &str = "SELECT * FROM parts JOIN uploads ON parts.hash = uploads.hash WHERE uploads.hash = ? AND parts.is_uploaded = ?";

    /// SQL statement to select a part by its hash identifier and part number
    const SELECT_PARTS_TABLE_BY_HASH_AND_PART_NUMBER: &str = "SELECT * FROM parts JOIN uploads ON parts.hash = uploads.hash WHERE uploads.hash = ? AND parts.part_number = ?";

    /// SQL statement to update an upload as complete
    const UPDATE_UPLOAD_AS_COMPLETE: &str = "UPDATE uploads SET is_complete = 1 WHERE hash = ?";

    /// SQL statement to update the file hash of an upload
    const UPDATE_UPLOAD_FILE_HASH: &str =
        "UPDATE uploads SET file_hash = ?, file_hash_algo = ? WHERE hash = ?";

    /// Creates a new upload instance with a generated hash
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base Dataverse URL
    /// * `pid` - Process ID associated with the upload
    /// * `path` - File path of the upload
    /// * `complete_url` - URL to call when completing the upload
    /// * `abort_url` - URL to call when aborting the upload
    /// * `size` - Size of the upload in bytes
    /// * `part_size` - Part size in bytes
    ///
    /// # Returns
    ///
    /// A new Uploads instance with a hash generated from the base_url, path, pid, and size
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: &str,
        storage_id: &str,
        pid: &str,
        path: &str,
        complete_url: &str,
        abort_url: &str,
        size: i64,
        part_size: i64,
        file_hash_algo: &str,
    ) -> Self {
        // Generate the hash from the path, pid and size
        let hash = Upload::create_hash(base_url, path, pid, size, file_hash_algo);

        Self {
            storage_id: storage_id.to_string(),
            base_url: base_url.to_string(),
            pid: pid.to_string(),
            path: path.to_string(),
            hash,
            is_complete: false,
            complete_url: complete_url.to_string(),
            abort_url: abort_url.to_string(),
            size,
            part_size,
            file_hash: None,
            file_hash_algo: file_hash_algo.to_string(),
        }
    }

    /// Creates a new upload instance with a generated hash
    ///
    /// This hash is used to identify the upload in the database and to
    /// associate the parts with the upload.
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base Dataverse URL
    /// * `pid` - Process ID associated with the upload
    /// * `path` - File path of the upload
    /// * `size` - Size of the upload in bytes
    ///
    /// # Returns
    ///
    /// A new Uploads instance with a hash generated from the base_url, path, pid, and size
    pub(crate) fn create_hash(
        base_url: impl Into<String>,
        path: impl Into<String>,
        pid: impl Into<String>,
        size: i64,
        file_hash_algo: &str,
    ) -> String {
        let base_url = base_url.into();
        let path = path.into();
        let pid = pid.into();
        let md5_hash = md5::compute(format!(
            "{}-{}-{}-{}-{}",
            base_url, path, pid, size, file_hash_algo
        ));
        format!("{:x}", md5_hash)
    }

    /// Creates a new upload instance from a MultiPartTicket
    ///
    /// # Arguments
    ///
    /// * `ticket` - The MultiPartTicket to create the upload from
    /// * `base_url` - Base Dataverse URL
    /// * `pid` - Process ID associated with the upload
    /// * `path` - File path of the upload
    /// * `size` - Size of the upload in bytes
    ///
    /// # Returns
    ///
    /// A new Uploads instance from the MultiPartTicket
    pub(crate) async fn from_ticket(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        ticket: &MultiPartTicket,
        base_url: &str,
        pid: &Identifier,
        path: &Path,
        size: u64,
        file_hash_algo: &str,
    ) -> Result<Upload, sqlx::Error> {
        let pid = match pid {
            Identifier::PersistentId(pid) => pid,
            _ => unimplemented!("Only PIDs are supported for now"),
        };

        let upload = Upload::new(
            base_url,
            &ticket.storage_identifier,
            pid,
            path.to_str().unwrap(),
            &ticket.complete,
            &ticket.abort,
            size as i64,
            ticket.part_size,
            file_hash_algo,
        );

        // Insert or get the upload
        let urls = ticket
            .urls
            .values()
            .map(|url| {
                TestMode::from_str(&std::env::var("TEST_MODE").unwrap_or_default())
                    .unwrap_or(TestMode::Off)
                    .process_url(url);
                url.clone()
            })
            .collect();
        Upload::insert_or_get(pool, &upload, urls).await
    }

    /// Creates the uploads table in the database if it doesn't exist
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn create_table(pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query(Self::CREATE_UPLOADS_TABLE)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Inserts a new upload into the database if it doesn't exist, otherwise returns the existing upload
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `upload` - The upload to insert
    /// * `urls` - Vector of parts to associate with this upload
    ///
    /// # Returns
    ///
    /// * `Result<Upload, sqlx::Error>` - The upload if successful, Err with the database error otherwise
    pub async fn insert_or_get(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        upload: &Upload,
        urls: Vec<impl Into<Part>>,
    ) -> Result<Upload, sqlx::Error> {
        // First, try to get the upload
        match Self::get_by_hash(pool, &upload.hash).await {
            Ok(upload) => Ok(upload),
            Err(sqlx::Error::RowNotFound) => Self::insert(pool, upload, urls).await,
            Err(e) => Err(e),
        }
    }

    /// Inserts a new upload into the database
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `upload` - The upload to insert
    /// * `urls` - Vector of parts to associate with this upload
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn insert(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        upload: &Upload,
        urls: Vec<impl Into<Part>>,
    ) -> Result<Upload, sqlx::Error> {
        sqlx::query(Self::INSERT_UPLOADS_TABLE)
            .bind(upload.hash.clone())
            .bind(upload.storage_id.clone())
            .bind(upload.base_url.clone())
            .bind(upload.pid.clone())
            .bind(upload.path.clone())
            .bind(upload.is_complete)
            .bind(upload.complete_url.clone())
            .bind(upload.abort_url.clone())
            .bind(upload.size)
            .bind(upload.part_size)
            .execute(pool)
            .await?;

        // Sort the parts by their part number
        let mut parts = urls
            .into_iter()
            .map(|url| url.into())
            .collect::<Vec<Part>>();
        parts.sort_by_key(|part| part.part_number.parse::<i64>().unwrap());

        // Calculate start and end bytes for each part based on part_size
        let mut current_byte = 0;
        for mut part in parts {
            part.hash = upload.hash.clone();
            part.start_byte = current_byte;
            part.end_byte = std::cmp::min(current_byte + upload.part_size - 1, upload.size - 1);
            part.file_path = upload.path.clone();
            current_byte = part.end_byte + 1;
            part.insert(pool).await?;
        }

        // Get the upload from the database
        let upload = Self::get_by_hash(pool, &upload.hash).await?;

        Ok(upload)
    }

    /// Retrieves all uploads from the database
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Uploads>, sqlx::Error>` - Vector of all uploads if successful, Err with the database error otherwise
    pub async fn get_all(pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<Vec<Upload>, sqlx::Error> {
        let uploads = sqlx::query_as::<_, Upload>(Self::SELECT_UPLOADS_TABLE)
            .fetch_all(pool)
            .await?;
        Ok(uploads)
    }

    /// Retrieves a single upload by its hash identifier
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `hash` - The hash identifier to search for
    ///
    /// # Returns
    ///
    /// * `Result<Uploads, sqlx::Error>` - The matching upload if found, Err with the database error otherwise
    pub async fn get_by_hash(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        hash: &str,
    ) -> Result<Upload, sqlx::Error> {
        let upload = sqlx::query_as::<_, Upload>(Self::SELECT_UPLOADS_TABLE_BY_HASH)
            .bind(hash)
            .fetch_one(pool)
            .await?;
        Ok(upload)
    }

    /// Retrieves all parts by their hash identifier
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `incomplete` - If true, only returns incomplete parts
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Parts>, sqlx::Error>` - Vector of all parts if successful, Err with the database error otherwise
    pub async fn get_parts(
        &self,
        pool: &sqlx::Pool<sqlx::Sqlite>,
        incomplete: bool,
    ) -> Result<Vec<Part>, sqlx::Error> {
        if incomplete {
            sqlx::query_as::<_, Part>(Self::SELECT_PARTS_TABLE_BY_HASH_AND_INCOMPLETE)
                .bind(self.hash.clone())
                .bind(false)
                .fetch_all(pool)
                .await
        } else {
            sqlx::query_as::<_, Part>(Self::SELECT_PARTS_TABLE_BY_HASH)
                .bind(self.hash.clone())
                .fetch_all(pool)
                .await
        }
    }

    /// Retrieves a part by its hash identifier and part number
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `part_number` - The part number to search for
    ///
    /// # Returns
    ///
    /// * `Result<Parts, sqlx::Error>` - The matching part if found, Err with the database error otherwise
    pub async fn get_part_by_number(
        &self,
        pool: &sqlx::Pool<sqlx::Sqlite>,
        part_number: impl Into<String>,
    ) -> Result<Part, sqlx::Error> {
        let part = sqlx::query_as::<_, Part>(Self::SELECT_PARTS_TABLE_BY_HASH_AND_PART_NUMBER)
            .bind(self.hash.clone())
            .bind(part_number.into())
            .fetch_one(pool)
            .await?;
        Ok(part)
    }

    /// Collects all etags from the parts of an upload
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<HashMap<String, String>, Box<dyn std::error::Error>>` - A map of part numbers to etags if successful, Err with the database error otherwise   
    pub async fn collect_etags(
        &self,
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<(HashMap<String, String>, String), Box<dyn std::error::Error>> {
        let parts = self.get_parts(pool, false).await?;
        let mut etags = HashMap::new();
        for part in parts {
            etags.insert(
                part.part_number.clone(),
                part.etag.ok_or(format!(
                    "Part {} has no etag. It might not be uploaded yet.",
                    part.part_number
                ))?,
            );
        }

        // Mark the upload as complete to avoid
        // other processes from using it
        self.set_complete(pool).await?;

        Ok((etags, self.complete_url.clone()))
    }

    /// Sets the upload as complete
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    async fn set_complete(&self, pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query(Self::UPDATE_UPLOAD_AS_COMPLETE)
            .bind(self.hash.clone())
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Sets the file hash of an upload
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `file_hash` - The file hash to set
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn set_file_hash(
        &self,
        file_hash: impl Into<String>,
        file_hash_algo: impl Into<String>,
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(Self::UPDATE_UPLOAD_FILE_HASH)
            .bind(file_hash.into())
            .bind(file_hash_algo.into())
            .bind(self.hash.clone())
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Deletes an upload from the database. Associated parts will be automatically deleted due to CASCADE
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `hash` - The hash identifier of the upload to delete
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn delete(pool: &sqlx::Pool<sqlx::Sqlite>, hash: &str) -> Result<(), sqlx::Error> {
        sqlx::query(Self::DELETE_UPLOADS_TABLE_BY_HASH)
            .bind(hash)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Deletes all uploads with expired parts
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `expiration_time` - The expiration time to delete the uploads. If None, uses current time
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn delete_expired(
        pool: &sqlx::Pool<sqlx::Sqlite>,
        expiration_time: Option<NaiveDateTime>,
    ) -> Result<(), sqlx::Error> {
        let all_uploads = Self::get_all(pool).await?;
        let expiration_time = expiration_time.unwrap_or(Utc::now().naive_utc());
        for upload in all_uploads {
            if upload.is_complete {
                // Complete uploads can be deleted immediately
                Self::delete(pool, &upload.hash).await?;
                continue;
            }

            // Check if any part has expired
            let parts = upload.get_parts(pool, false).await?;
            if parts.iter().any(|part| {
                NaiveDateTime::parse_from_str(&part.expires_at, "%Y-%m-%d %H:%M:%S%.f UTC")
                    .map(|expires_at| expires_at < expiration_time)
                    .unwrap_or(true) // Delete if we can't parse the date (safer)
            }) {
                Self::delete(pool, &upload.hash).await?;
            }
        }
        Ok(())
    }

    /// Removes all uploads that have expired or are complete
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn remove_finished_and_expired_uploads(
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<(), sqlx::Error> {
        // Delete all uploads that have expired
        Self::delete_expired(pool, None).await?;

        // Delete all uploads that are complete
        let all_uploads = Self::get_all(pool).await?;
        for upload in all_uploads {
            if upload.is_complete {
                Self::delete(pool, &upload.hash).await?;
            }
        }
        Ok(())
    }
}

/// Represents a part of a multi-part upload in the database
///
/// Each part is associated with an upload via the hash field.
/// Parts contain information about their expiration, ETag (entity tag),
/// and part number within the multi-part upload.
///
#[derive(Clone, FromRow, Debug)]
pub struct Part {
    /// Unique identifier for the part
    pub id: i64,
    /// Hash identifier linking to the parent upload
    pub hash: String,
    /// URL of the part
    pub url: String,
    /// Expiration timestamp for the part
    pub expires_at: String,
    /// Optional ETag (entity tag) for the part
    pub etag: Option<String>,
    /// Part number within the multi-part upload
    pub part_number: String,
    /// Flag indicating whether the part is uploaded
    pub is_uploaded: bool,
    /// Start byte of the part
    pub start_byte: i64,
    /// End byte of the part
    pub end_byte: i64,
    /// File path for the part
    pub file_path: String,
}

impl Part {
    /// SQL statement to create the parts table if it doesn't exist
    const CREATE_PARTS_TABLE: &str = "CREATE TABLE IF NOT EXISTS parts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT NOT NULL,
        url TEXT NOT NULL,
        start_byte INTEGER NOT NULL,
        end_byte INTEGER NOT NULL,
        expires_at TEXT NOT NULL,
        etag TEXT,
        part_number TEXT NOT NULL,
        is_uploaded BOOLEAN NOT NULL,
        file_path TEXT NOT NULL,
        FOREIGN KEY (hash) REFERENCES uploads(hash) ON DELETE CASCADE
    )";

    /// SQL statement to insert a new part into the database
    const INSERT_PARTS_TABLE: &str = "INSERT INTO parts (hash, url, start_byte, end_byte, expires_at, etag, part_number, is_uploaded, file_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// SQL statement to update a part as complete
    const UPDATE_PART_AS_UPLOADED: &str = "UPDATE parts SET is_uploaded = 1, etag = ? WHERE id = ?";

    /// SQL statement to select all parts from the database
    const SELECT_PARTS_TABLE: &str = "SELECT * FROM parts";

    /// SQL statement to get the parts upload file
    const SELECT_PARTS_TABLE_AS_UPLOAD_FILE: &str = "SELECT * FROM parts WHERE hash = ?";

    /// Creates the parts table in the database if it doesn't exist
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn create_table(pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query(Self::CREATE_PARTS_TABLE).execute(pool).await?;
        Ok(())
    }

    /// Inserts a new part into the database
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn insert(&self, pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query(Self::INSERT_PARTS_TABLE)
            .bind(self.hash.clone())
            .bind(self.url.clone())
            .bind(self.start_byte)
            .bind(self.end_byte)
            .bind(self.expires_at.clone())
            .bind(self.etag.clone())
            .bind(self.part_number.clone())
            .bind(self.is_uploaded)
            .bind(self.file_path.clone())
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Updates a part as complete
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<sqlx::SqliteQueryResult, sqlx::Error>` - Ok if successful, Err with the database error otherwise   
    pub async fn update_part_as_complete(
        &self,
        etag: impl Into<String>,
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<SqliteQueryResult, sqlx::Error> {
        sqlx::query(Self::UPDATE_PART_AS_UPLOADED)
            .bind(etag.into())
            .bind(self.id)
            .execute(pool)
            .await
    }

    /// Retrieves all parts from the database
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Parts>, sqlx::Error>` - Vector of all parts if successful, Err with the database error otherwise
    pub async fn get_all(pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<Vec<Part>, sqlx::Error> {
        let parts = sqlx::query_as::<_, Part>(Self::SELECT_PARTS_TABLE)
            .fetch_all(pool)
            .await?;
        Ok(parts)
    }

    /// Uploads a part of a multi-part upload
    ///
    /// # Arguments
    ///
    /// * `client` - The client to use to upload the part
    /// * `callbacks` - The callbacks to use to upload the part
    ///
    /// # Returns
    ///
    /// * `Result<String, String>` - The ETag of the part if successful, Err with the error otherwise
    pub async fn upload_part(
        &self,
        client: &BaseClient,
        callbacks: Option<Vec<CallbackFun>>,
    ) -> Result<String, String> {
        let file = UploadFile::from(self);
        let context = RequestType::File { file, callbacks };

        // Get URL based on test mode
        let test_mode = std::env::var("TEST_MODE").unwrap_or_default();
        let url = TestMode::from_str(&test_mode)
            .unwrap_or(TestMode::Off)
            .process_url(&self.url);

        // Make the request and handle the response
        match client.put(&url, None, context, None).await {
            Err(e) => Err(e.to_string()),
            Ok(response) => {
                if response.status().is_success() {
                    response
                        .headers()
                        .get("etag")
                        .ok_or("No ETag header")?
                        .to_str()
                        .map(String::from)
                        .map_err(|e| e.to_string())
                } else {
                    Err(response.text().await.unwrap_or_default())
                }
            }
        }
    }

    /// Set the part as complete
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<(), sqlx::Error>` - Ok if successful, Err with the database error otherwise
    pub async fn set_uploaded(
        &self,
        etag: String,
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(Self::UPDATE_PART_AS_UPLOADED)
            .bind(etag)
            .bind(self.id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Retrieves the parts upload file
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    ///
    /// # Returns
    ///
    /// * `Result<UploadFile, sqlx::Error>` - The parts upload file if successful, Err with the database error otherwise
    pub async fn get_upload_file(
        &self,
        pool: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<Upload, sqlx::Error> {
        let upload = sqlx::query_as::<_, Upload>(Self::SELECT_PARTS_TABLE_AS_UPLOAD_FILE)
            .bind(self.hash.clone())
            .fetch_one(pool)
            .await?;
        Ok(upload)
    }
}

impl FromStr for Part {
    type Err = Box<dyn std::error::Error>;

    /// Parses a URL string into a Parts struct
    ///
    /// # Arguments
    ///
    /// * `url` - The URL string to parse
    ///
    /// # Returns
    ///
    /// * `Result<Self, Self::Err>` - The parsed Parts struct or an error
    ///
    /// # Errors
    ///
    /// Returns an error if the URL cannot be parsed or if required query parameters are missing
    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(url)?;
        let pairs = url
            .query_pairs()
            .collect::<HashMap<Cow<'_, str>, Cow<'_, str>>>();

        let part_number = pairs
            .get("partNumber")
            .ok_or("partNumber not found")?
            .to_string();

        // Use chrono to fetch the current time and add the expires_at value
        let expires_at = pairs
            .get("X-Amz-Expires")
            .ok_or("X-Amz-Expires not found")?
            .to_string();

        let expires_at = Utc::now()
            .checked_add_signed(Duration::seconds(expires_at.parse::<i64>()?))
            .ok_or("Invalid expires_at value")?
            .to_string();

        Ok(Part {
            id: 0,
            hash: "".to_string(),
            url: url.to_string(),
            expires_at,
            etag: None,
            part_number,
            is_uploaded: false,
            start_byte: 0,
            end_byte: 0,
            file_path: "".to_string(),
        })
    }
}

impl From<&Part> for UploadFile {
    /// Convert a part to an upload file
    ///
    /// # Arguments
    ///
    /// * `part` - The part to convert
    ///
    /// # Returns
    ///
    /// * `UploadFile` - The converted upload file
    fn from(part: &Part) -> Self {
        let file_path = PathBuf::from(part.file_path.clone());
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let size = part.end_byte as u64 - part.start_byte as u64;
        UploadFile {
            name: file_name.to_string(),
            dir: None,
            file: FileSource::Path(file_path),
            size,
            start: Some(part.start_byte as u64),
            end: Some(part.end_byte as u64),
        }
    }
}

impl From<String> for Part {
    /// Convert a string to a Parts struct
    ///
    /// # Arguments
    ///
    /// * `url` - The string to convert
    ///
    /// # Returns
    ///
    /// * `Parts` - The converted Parts struct
    fn from(url: String) -> Self {
        Self::from_str(&url).unwrap()
    }
}

/// Test module for the database models
///
/// This module contains tests for the `Uploads` and `Parts` structs, testing their
/// functionality for database operations including:
/// - Creating and retrieving uploads and parts
/// - Handling expired parts
/// - Setting parts as complete
/// - Collecting ETags from parts
/// - Error handling for incomplete uploads
#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};

    use httpmock::MockServer;

    use super::*;

    // Example URL used to test the from_str function
    const EXAMPLE_URL: &str = "http://localstack:4566/mybucket/10.5072/FK2/SUHYR2/19633b6caba-889694f3fcd0?uploadId=Some&partNumber=2&X-Amz-Algorithm=Some&X-Amz-Date=Some&X-Amz-SignedHeaders=host&X-Amz-Expires=3599&X-Amz-Credential=Some&X-Amz-Signature=Some";

    /// Sets up an in-memory SQLite database for testing
    ///
    /// Creates a new in-memory SQLite database and initializes the required tables
    /// for uploads and parts.
    ///
    /// # Returns
    ///
    /// * `sqlx::Pool<sqlx::Sqlite>` - A connection pool to the in-memory database
    async fn setup_db() -> sqlx::Pool<sqlx::Sqlite> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        Upload::create_table(&pool)
            .await
            .expect("Failed to create table");
        Part::create_table(&pool)
            .await
            .expect("Failed to create table");
        pool
    }

    /// Tests inserting an upload without any associated parts
    ///
    /// Verifies that an upload can be created and retrieved without any parts,
    /// and that the parts collection for this upload is empty.
    #[tokio::test]
    async fn test_insert_upload_without_parts() {
        let pool = setup_db().await;
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/test.txt",
            "https://example.com/test.txt",
            100,
            10,
            "md5",
        );

        let parts: Vec<Part> = vec![];
        Upload::insert(&pool, &upload, parts).await.unwrap();

        let uploads = Upload::get_all(&pool).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].hash, upload.hash);
        assert_eq!(uploads[0].storage_id, upload.storage_id);
        let parts = upload.get_parts(&pool, false).await.unwrap();
        assert_eq!(parts.len(), 0);
    }

    /// Tests inserting an upload with associated parts
    ///
    /// Verifies that an upload can be created with parts, and that both the upload
    /// and its parts can be retrieved correctly from the database.
    #[tokio::test]
    async fn test_insert_upload_with_parts() {
        let pool = setup_db().await;
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/test.txt",
            "https://example.com/test.txt",
            100,
            10,
            "md5",
        );

        let mut part = Part::from_str(EXAMPLE_URL).unwrap();
        part.file_path = "test.txt".to_string();
        let parts = vec![part];

        Upload::insert(&pool, &upload, parts).await.unwrap();

        let uploads = Upload::get_all(&pool).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].hash, upload.hash);

        let parts = upload.get_parts(&pool, false).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, "2");
        assert_eq!(parts[0].file_path, "test.txt");
    }

    /// Tests the deletion of uploads with expired parts
    ///
    /// Creates two uploads:
    /// 1. One with a part that has already expired
    /// 2. One with a part that expires in the future
    ///
    /// Then verifies that only the expired upload is deleted when calling
    /// `delete_expired`, while the non-expired upload remains in the database.
    #[tokio::test]
    async fn test_delete_expired_parts() {
        let pool = setup_db().await;

        // Create two uploads with different expiration times
        // First one expires immediately
        let upload1 = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/test.txt",
            "https://example.com/test.txt",
            100,
            10,
            "md5",
        );

        // Create a part with the example URL and expire it immediately
        let mut part = Part::from_str(EXAMPLE_URL).unwrap();
        part.expires_at = Utc::now().to_string();

        Upload::insert(&pool, &upload1, vec![part]).await.unwrap();

        // Second one expires in 1 day
        let upload2 = Upload::new(
            "https://example2.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/test.txt",
            "https://example.com/test.txt",
            100,
            10,
            "md5",
        );

        // Part expires in 1 day
        let mut part = Part::from_str(EXAMPLE_URL).unwrap();
        part.expires_at = Utc::now()
            .checked_add_signed(Duration::days(1))
            .unwrap()
            .to_string();

        let parts = vec![part];
        Upload::insert(&pool, &upload2, parts).await.unwrap();

        // Fetch all uploads
        let uploads = Upload::get_all(&pool).await.unwrap();
        assert_eq!(uploads.len(), 2);

        // Now delete expired uploads
        Upload::delete_expired(&pool, None).await.unwrap();

        let uploads = Upload::get_all(&pool).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].hash, upload2.hash);

        let remaining_upload = Upload::get_by_hash(&pool, &upload2.hash).await.unwrap();
        let parts = remaining_upload.get_parts(&pool, false).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, "2");
    }

    /// Tests setting a part as complete
    ///
    /// Verifies that a part can be marked as uploaded with an ETag,
    /// and that the updated part can be retrieved with the correct values.
    #[tokio::test]
    async fn test_set_part_as_uploaded() {
        let pool = setup_db().await;

        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/test.txt",
            "https://example.com/test.txt",
            100,
            10,
            "md5",
        );

        let mut part = Part::from_str(EXAMPLE_URL).expect("Failed to parse URL");
        part.file_path = "test.txt".to_string();
        let parts = vec![part];

        Upload::insert(&pool, &upload, parts)
            .await
            .expect("Failed to insert upload");

        let part = upload
            .get_part_by_number(&pool, "2")
            .await
            .expect("Failed to get part by number");

        assert_eq!(part.file_path, "test.txt");

        part.update_part_as_complete("etag", &pool)
            .await
            .expect("Failed to update part as complete");

        let parts = upload
            .get_parts(&pool, false)
            .await
            .expect("Failed to get parts");

        assert_eq!(parts.len(), 1);
        assert!(parts[0].is_uploaded);
        assert_eq!(parts[0].etag, Some("etag".to_string()));
        assert_eq!(parts[0].file_path, "test.txt");
    }

    /// Tests the collection of etags from an upload
    ///
    /// Verifies that ETags can be collected from all parts of an upload
    /// when all parts have been uploaded and have ETags.
    #[tokio::test]
    async fn test_collect_etags() {
        let pool = setup_db().await;

        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        // Create two parts with an etag
        let mut part1 = Part::from_str(EXAMPLE_URL).unwrap();
        part1.part_number = "1".to_string();
        part1.etag = Some("etag1".to_string());

        let mut part2 = Part::from_str(EXAMPLE_URL).unwrap();
        part2.part_number = "2".to_string();
        part2.etag = Some("etag2".to_string());

        let parts = vec![part1, part2];
        Upload::insert(&pool, &upload, parts).await.unwrap();

        let (etags, complete_url) = upload.collect_etags(&pool).await.unwrap();
        assert_eq!(etags.len(), 2);
        assert_eq!(etags["1"], "etag1");
        assert_eq!(etags["2"], "etag2");
        assert_eq!(complete_url, "https://example.com/complete");
    }

    /// Tests collection of etags from an incomplete upload
    ///
    /// Verifies that attempting to collect ETags from an upload where
    /// not all parts have ETags results in an error.
    #[tokio::test]
    async fn test_collect_etags_not_complete() {
        let pool = setup_db().await;

        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        let mut part1 = Part::from_str(EXAMPLE_URL).unwrap();
        part1.part_number = "1".to_string();
        part1.etag = Some("etag1".to_string());

        let mut part2 = Part::from_str(EXAMPLE_URL).unwrap();
        part2.part_number = "2".to_string();

        let parts = vec![part1, part2];
        Upload::insert(&pool, &upload, parts).await.unwrap();

        upload
            .collect_etags(&pool)
            .await
            .expect_err("Etag collection should fail due to incomplete upload");
    }

    /// Tests the removal of finished and expired uploads
    ///
    /// Verifies that all uploads that have expired or are complete are deleted
    /// when calling `remove_finished_and_expired_uploads`.
    #[tokio::test]
    async fn test_remove_finished_and_expired_uploads() {
        let pool = setup_db().await;

        // Create an upload that has been completed
        let mut upload1 = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test1.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );
        upload1.is_complete = true;
        let parts = vec![Part::from_str(EXAMPLE_URL).unwrap()];
        Upload::insert(&pool, &upload1, parts).await.unwrap();

        // Create an upload that has expired
        let upload2 = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test2.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        let mut part = Part::from_str(EXAMPLE_URL).unwrap();
        part.expires_at = Utc::now().to_string();

        let parts = vec![part];
        Upload::insert(&pool, &upload2, parts).await.unwrap();

        // Create an upload that has not expired
        let upload3 = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test3.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        let parts = vec![Part::from_str(EXAMPLE_URL).unwrap()];
        Upload::insert(&pool, &upload3, parts).await.unwrap();

        // Call the function to remove finished and expired uploads
        Upload::remove_finished_and_expired_uploads(&pool)
            .await
            .unwrap();

        // Verify that only the expired upload is deleted
        let uploads = Upload::get_all(&pool).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].hash, upload3.hash);
    }

    #[tokio::test]
    async fn test_part_byte_ranges() {
        // Create a test database
        let pool = setup_db().await;

        // Create an upload with a specific size and part size
        let file_size = 100;
        let part_size = 20; // Using part size of 20
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test_bytes.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            file_size,
            part_size,
            "md5",
        );

        // Create parts with correct part numbers
        let mut parts = vec![
            Part::from_str(EXAMPLE_URL).unwrap(),
            Part::from_str(EXAMPLE_URL).unwrap(),
            Part::from_str(EXAMPLE_URL).unwrap(),
            Part::from_str(EXAMPLE_URL).unwrap(),
            Part::from_str(EXAMPLE_URL).unwrap(),
        ];

        for (i, part) in parts.iter_mut().enumerate() {
            part.part_number = (i + 1).to_string();
        }

        // Insert the upload and parts
        Upload::insert(&pool, &upload, parts).await.unwrap();

        // Retrieve the parts to verify byte ranges
        let retrieved_parts = upload.get_parts(&pool, false).await.unwrap();

        // Verify the number of parts
        assert_eq!(retrieved_parts.len(), 5);

        // Verify the byte ranges for each part
        assert_eq!(retrieved_parts[0].start_byte, 0);
        assert_eq!(retrieved_parts[0].end_byte, 19);

        assert_eq!(retrieved_parts[1].start_byte, 20);
        assert_eq!(retrieved_parts[1].end_byte, 39);

        assert_eq!(retrieved_parts[2].start_byte, 40);
        assert_eq!(retrieved_parts[2].end_byte, 59);

        assert_eq!(retrieved_parts[3].start_byte, 60);
        assert_eq!(retrieved_parts[3].end_byte, 79);

        assert_eq!(retrieved_parts[4].start_byte, 80);
        assert_eq!(retrieved_parts[4].end_byte, 99);
    }

    #[tokio::test]
    async fn test_set_file_hash() {
        let pool = setup_db().await;

        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        let parts = vec![Part::from_str(EXAMPLE_URL).unwrap()];
        Upload::insert(&pool, &upload, parts).await.unwrap();

        upload
            .set_file_hash("1234567890", "md5", &pool)
            .await
            .expect("Failed to set file hash");

        let uploaded_upload = Upload::get_by_hash(&pool, &upload.hash)
            .await
            .expect("Failed to get upload");
        assert_eq!(uploaded_upload.file_hash, Some("1234567890".to_string()));
    }

    #[tokio::test]
    async fn test_insert_or_get() {
        let pool = setup_db().await;
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            "test.txt",
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        // We clone the upload to ensure the hash is the same,
        // but the file hash is not explicitly set
        // This way we can figure out if the insert_or_get
        // is working correctly
        let upload_clone = upload.clone();

        // First insert a new upload
        let parts = vec![Part::from_str(EXAMPLE_URL).unwrap()];
        let upload = Upload::insert_or_get(&pool, &upload, parts.clone())
            .await
            .unwrap();

        // Now set the file hash
        upload
            .set_file_hash("1234567890", "md5", &pool)
            .await
            .expect("Failed to set file hash");

        // Now try to insert the same upload again
        // We expect the file hash to be set, since it was set
        // in the previous insert
        let uploaded_upload = Upload::insert_or_get(&pool, &upload_clone, parts)
            .await
            .unwrap();

        assert!(upload_clone.file_hash.is_none());
        assert_eq!(uploaded_upload.hash, upload_clone.hash);
        assert_eq!(uploaded_upload.file_hash, Some("1234567890".to_string()));
    }

    #[tokio::test]
    async fn test_upload_part() {
        // Setup test environment
        let pool = setup_db().await;
        let server = MockServer::start();

        // Create a mock file
        let mock_dir = tempfile::tempdir().unwrap();
        let mock_path = mock_dir.path().join("test.txt");
        File::create(&mock_path)
            .unwrap()
            .write_all(b"Hello, world!")
            .unwrap();

        // Configure mock server endpoint
        let part_endpoint = server.mock(|when, then| {
            when.method(httpmock::Method::PUT)
                .path("/part1")
                .query_param("partNumber", "2")
                .query_param("X-Amz-Expires", "3599");
            then.status(200).header("ETag", "etag");
        });

        // Create part URL and client
        let part_url = server.url("/part1?partNumber=2&X-Amz-Expires=3599");
        let client = BaseClient::new(&server.url("/"), None).expect("Failed to create client");

        // Create and insert upload with part
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            mock_path.to_str().unwrap(),
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "md5",
        );

        let parts = vec![Part::from_str(&part_url).unwrap()];
        Upload::insert_or_get(&pool, &upload, parts)
            .await
            .expect("Failed to insert upload");

        // Retrieve upload and part
        let upload = Upload::get_by_hash(&pool, &upload.hash)
            .await
            .expect("Failed to get upload");

        let parts = upload
            .get_parts(&pool, false)
            .await
            .expect("Failed to get parts");

        let part = parts.first().expect("No parts found");

        // Test part upload
        let etag = part
            .upload_part(&client, None)
            .await
            .expect("Failed to upload part");

        // Verify results
        assert_eq!(etag, "etag");
        part_endpoint.assert();
    }
}
