//! A Rust library for interacting with the Dataverse API.
//!
//! This library provides a comprehensive set of tools and utilities for working with
//! Dataverse repositories, including functionality for managing datasets, files,
//! collections, and more.

#![warn(unused_crate_dependencies)]

/// Client functionality for interacting with Dataverse APIs
pub mod client;

/// Types and utilities for working with dataset versions
pub mod datasetversion;

/// Types for handling Dataverse identifiers
pub mod identifier;

/// Progress tracking utilities
pub(crate) mod progress;

/// Types for making requests to Dataverse APIs
pub mod request;

/// Types for handling responses from Dataverse APIs
pub mod response;

/// General utility functions
pub mod utils;

/// Macros for checking locks
pub mod macros;

/// File handling functionality
pub mod file {
    pub use mime::infer_mime;
    pub use uploadfile::UploadFile;

    /// Callback functionality for file operations
    pub mod callback;
    /// File streaming utilities
    pub(crate) mod filestream;
    /// MIME type inference
    pub(crate) mod mime;
    /// File upload functionality
    pub(crate) mod uploadfile;
    /// ZIP file streaming utilities
    pub(crate) mod zipstream;
}

/// Data access functionality
pub mod data_access {
    pub use datafile::download_datafile;
    /// Datafile operations
    pub mod datafile;
    /// Dataset operations
    pub mod dataset;
}

/// Search API functionality
pub mod search_api {
    pub use search::search;

    /// Search query building
    pub mod query;
    /// Search execution
    pub mod search;
}

/// Direct upload functionality
pub mod direct_upload {
    pub use upload::batch_direct_upload;
    pub use upload::direct_upload;

    /// Database operations
    mod db;
    /// Hasher functionality
    pub mod hasher;
    /// Database model
    pub mod model;
    /// Upload ticket handling
    pub(crate) mod ticket;
    /// Direct upload operations
    pub mod upload;
}

/// Native API functionality
pub mod native_api {
    /// Template types for API operations
    pub mod templates {
        pub use super::dataset::edit::EditMetadataBody;
        pub use super::dataset::upload::UploadBody;
    }

    /// Administrative operations
    pub mod admin {
        /// Collection administration
        pub mod collection {
            /// Storage management
            pub mod storage;
        }
    }

    /// Collection operations
    pub mod collection {
        pub use content::get_content;
        pub use create::create_collection;
        pub use delete::delete_collection;
        pub use publish::publish_collection;

        /// Collection content operations
        pub mod content;
        /// Collection creation
        pub mod create;
        /// Collection deletion
        pub mod delete;
        /// Collection publishing
        pub mod publish;
    }

    /// Information retrieval operations
    pub mod info {
        pub use exporters::get_exporters;
        pub use version::get_version;

        /// Version information
        pub mod version;

        /// Exporters information
        pub mod exporters;
    }

    /// Dataset operations
    pub mod dataset {
        pub use create::create_dataset;
        pub use delete::delete_dataset;
        pub use dirupload::upload_directory;
        pub use edit::edit_dataset_metadata;
        pub use export::export_dataset;
        pub use link::link_dataset;
        pub use listfiles::list_dataset_files;
        pub use locks::{get_locks, remove_lock, set_lock};
        pub use metadata::get_dataset_meta;
        pub use review::{return_to_author, submit_for_review};
        pub use upload::upload_file_to_dataset;

        /// Dataset creation
        pub mod create;
        /// Dataset deletion
        pub mod delete;
        /// Directory upload functionality
        pub mod dirupload;
        /// Dataset editing
        pub mod edit;
        /// Dataset export operations
        pub mod export;
        /// Dataset linking
        pub mod link;
        /// File listing
        pub mod listfiles;
        /// Locks operations
        pub mod locks;
        /// Metadata operations
        pub mod metadata;
        /// Publishing operations
        pub mod publish;
        /// Dataset review operations
        pub mod review;
        /// Size calculations
        pub mod size;
        /// File upload operations
        pub mod upload;
    }

    /// File operations
    pub mod file {
        pub use metadata::get_file_meta;
        pub use replace::replace_file;

        /// File metadata operations
        pub mod metadata;
        /// File replacement operations
        pub mod replace;
    }
}

/// Commonly used types and functions
pub mod prelude {
    pub use crate::file::callback::CallbackFun;

    pub use super::client::BaseClient;
    pub use super::datasetversion::DatasetVersion;
    pub use super::file::UploadFile;
    pub use super::identifier::Identifier;
    pub use super::native_api::collection;
    pub use super::native_api::dataset;
    pub use super::native_api::file;
    pub use super::native_api::info;
}

/// Command-line interface functionality
pub mod cli {
    /// Administrative commands
    pub mod admin;
    /// Authentication commands
    pub mod auth;
    /// Base CLI functionality
    pub mod base;
    /// Collection commands
    pub mod collection;
    /// Dataset commands
    pub mod dataset;
    /// File commands
    pub mod file;
    /// Information commands
    pub mod info;
}

/// Test utilities
#[cfg(test)]
mod test_utils;
