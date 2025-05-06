use std::error::Error;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::file::uploadfile::FileSource;
use crate::file::UploadFile;
use crate::prelude::CallbackFun;

use super::upload::{Checksum, DirectUploadBody};

const MD5: &str = "MD5";
const SHA256: &str = "SHA-256";
const SHA512: &str = "SHA-512";
const SHA1: &str = "SHA-1";

/// Trait defining the interface for hash computation and management.
///
/// This trait provides methods for consuming data to be hashed, computing the final hash value,
/// creating callbacks for streaming hash computation, and adding hash information to upload payloads.
pub trait Hasher {
    /// Consumes a chunk of data to be included in the hash computation.
    ///
    /// # Arguments
    ///
    /// * `data` - A byte slice containing the data to be hashed
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if the operation failed
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>>;

    /// Computes the final hash value as a hexadecimal string.
    ///
    /// # Returns
    ///
    /// A `Result` containing the computed hash as a hexadecimal string, or an error if the computation failed
    fn compute(&self) -> Result<String, Box<dyn Error + '_>>;

    /// Creates a callback function that can be used for streaming hash computation.
    ///
    /// # Returns
    ///
    /// A `CallbackFun` that can be called with chunks of data to update the hash state
    fn to_callback(&self) -> CallbackFun;

    /// Adds the computed hash to the provided upload payload.
    ///
    /// # Arguments
    ///
    /// * `payload` - A mutable reference to the `DirectUploadBody` to which the hash will be added
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if the operation failed
    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>>;

    /// Returns the algorithm used to hash the file
    fn algorithm(&self) -> &str;
}

/// Enum representing different hash algorithms supported by the system.
///
/// This enum allows for polymorphic handling of different hash algorithms while
/// providing a unified interface through the `Hasher` trait.
pub enum FileHash {
    /// MD5 hash algorithm implementation
    MD5(MD5Hasher),
    /// SHA-256 hash algorithm implementation
    SHA256(SHA256Hasher),
    /// SHA-512 hash algorithm implementation
    SHA512(SHA512Hasher),
    /// SHA-1 hash algorithm implementation
    SHA1(SHA1Hasher),
}

impl FileHash {
    pub fn new(algorithm: &str) -> Result<Self, String> {
        match algorithm {
            "MD5" => Ok(FileHash::MD5(MD5Hasher::new())),
            "SHA-256" => Ok(FileHash::SHA256(SHA256Hasher::new())),
            "SHA-512" => Ok(FileHash::SHA512(SHA512Hasher::new())),
            "SHA-1" => Ok(FileHash::SHA1(SHA1Hasher::new())),
            _ => Err("Invalid hash algorithm".to_string()),
        }
    }
}

impl Clone for FileHash {
    // For this clone method, we need to create a new instance of the FileHash struct
    // with the same algorithm. This is because the Derive Clone for FileHash struct
    // will not work as expected because the hasher state is shared across threads.
    fn clone(&self) -> Self {
        match self {
            FileHash::MD5(_) => FileHash::new("MD5").unwrap(),
            FileHash::SHA256(_) => FileHash::new("SHA-256").unwrap(),
            FileHash::SHA512(_) => FileHash::new("SHA-512").unwrap(),
            FileHash::SHA1(_) => FileHash::new("SHA-1").unwrap(),
        }
    }
}

impl FromStr for FileHash {
    type Err = String;

    /// Creates a `FileHash` instance from a string representation of the hash algorithm.
    ///
    /// # Arguments
    ///
    /// * `s` - A string slice containing the name of the hash algorithm
    ///
    /// # Returns
    ///
    /// A `Result` containing the corresponding `FileHash` variant or an error if the algorithm is not supported
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "MD5" => Ok(FileHash::MD5(MD5Hasher::new())),
            "SHA-256" => Ok(FileHash::SHA256(SHA256Hasher::new())),
            "SHA-512" => Ok(FileHash::SHA512(SHA512Hasher::new())),
            "SHA-1" => Ok(FileHash::SHA1(SHA1Hasher::new())),
            _ => Err("Invalid hash algorithm".to_string()),
        }
    }
}

impl TryFrom<&str> for FileHash {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        FileHash::new(s)
    }
}

impl Hasher for FileHash {
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>> {
        match self {
            FileHash::MD5(hasher) => hasher.consume(data),
            FileHash::SHA256(hasher) => hasher.consume(data),
            FileHash::SHA512(hasher) => hasher.consume(data),
            FileHash::SHA1(hasher) => hasher.consume(data),
        }
    }

    fn compute(&self) -> Result<String, Box<dyn Error + '_>> {
        match self {
            FileHash::MD5(hasher) => hasher.compute(),
            FileHash::SHA256(hasher) => hasher.compute(),
            FileHash::SHA512(hasher) => hasher.compute(),
            FileHash::SHA1(hasher) => hasher.compute(),
        }
    }

    fn to_callback(&self) -> CallbackFun {
        match self {
            FileHash::MD5(hasher) => hasher.to_callback(),
            FileHash::SHA256(hasher) => hasher.to_callback(),
            FileHash::SHA512(hasher) => hasher.to_callback(),
            FileHash::SHA1(hasher) => hasher.to_callback(),
        }
    }

    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>> {
        match self {
            FileHash::MD5(hasher) => hasher.add_to_payload(payload),
            FileHash::SHA256(hasher) => hasher.add_to_payload(payload),
            FileHash::SHA512(hasher) => hasher.add_to_payload(payload),
            FileHash::SHA1(hasher) => hasher.add_to_payload(payload),
        }
    }

    fn algorithm(&self) -> &str {
        match self {
            FileHash::MD5(hasher) => hasher.algorithm(),
            FileHash::SHA256(hasher) => hasher.algorithm(),
            FileHash::SHA512(hasher) => hasher.algorithm(),
            FileHash::SHA1(hasher) => hasher.algorithm(),
        }
    }
}

impl FileHash {
    /// Hashes the contents of an `UploadFile` asynchronously.
    ///
    /// This method consumes the contents of the provided `UploadFile` and updates
    /// the hash state of the `FileHash` instance. Please note that this method is only
    /// supported for local files (as `FileSource::File`) and paths (as `FileSource::Path`).
    ///
    /// # Arguments
    ///
    /// * `file` - The `UploadFile` to be hashed
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if the operation failed
    pub async fn hash(&self, file: impl Into<UploadFile>) -> Result<(), String> {
        let mut file = match file.into().file {
            FileSource::File(file) => file,
            FileSource::Path(path) => File::open(path).await.map_err(|e| e.to_string())?,
            _ => return Err("Unsupported file type".into()),
        };

        // Compute the final hash value
        let mut buffer = vec![0; 64 * 1024]; // 64KB buffer
        loop {
            let bytes_read = file.read(&mut buffer).await.map_err(|e| e.to_string())?;
            if bytes_read == 0 {
                break; // EOF reached
            }
            self.consume(&buffer[..bytes_read])
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }
}

/// Implementation of the MD5 hash algorithm.
///
/// This struct wraps an MD5 context in a thread-safe container to allow for
/// concurrent access and modification during streaming hash computation.
#[derive(Clone)]
pub struct MD5Hasher(Arc<Mutex<md5::Context>>);

impl MD5Hasher {
    /// Creates a new instance of the MD5 hasher.
    ///
    /// # Returns
    ///
    /// A new `MD5Hasher` instance with an initialized MD5 context
    pub fn new() -> Self {
        MD5Hasher(Arc::new(Mutex::new(md5::Context::new())))
    }
}

impl Hasher for MD5Hasher {
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>> {
        let mut hasher = self.0.lock()?;
        hasher.consume(data);
        Ok(())
    }

    fn compute(&self) -> Result<String, Box<dyn Error + '_>> {
        let hasher = self.0.lock()?.clone();
        Ok(format!("{:x}", hasher.compute()))
    }

    fn to_callback(&self) -> CallbackFun {
        let hasher_arc = self.0.clone();
        CallbackFun::new(Box::new(move |data: &[u8]| {
            let mut hasher = hasher_arc.lock().expect("Failed to lock hasher");
            hasher.consume(data);
        }))
    }

    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>> {
        add_checksum(payload, MD5, self.compute()?)
    }

    fn algorithm(&self) -> &str {
        MD5
    }
}

/// Implementation of the SHA-256 hash algorithm.
///
/// This struct wraps a SHA-256 context in a thread-safe container to allow for
/// concurrent access and modification during streaming hash computation.
#[derive(Clone)]
pub struct SHA256Hasher(Arc<Mutex<Sha256>>);

impl SHA256Hasher {
    /// Creates a new instance of the SHA-256 hasher.
    ///
    /// # Returns
    ///
    /// A new `SHA256Hasher` instance with an initialized SHA-256 context
    pub fn new() -> Self {
        SHA256Hasher(Arc::new(Mutex::new(Sha256::new())))
    }
}

impl Hasher for SHA256Hasher {
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>> {
        let mut hasher = self.0.lock()?;
        hasher.update(data);
        Ok(())
    }

    fn compute(&self) -> Result<String, Box<dyn Error + '_>> {
        let hasher = self.0.lock()?.clone();
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn to_callback(&self) -> CallbackFun {
        let hasher_arc = self.0.clone();
        CallbackFun::new(Box::new(move |data: &[u8]| {
            let mut hasher = hasher_arc.lock().expect("Failed to lock hasher");
            hasher.update(data);
        }))
    }

    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>> {
        add_checksum(payload, SHA256, self.compute()?)
    }

    fn algorithm(&self) -> &str {
        SHA256
    }
}

/// Implementation of the SHA-512 hash algorithm.
///
/// This struct wraps a SHA-512 context in a thread-safe container to allow for
/// concurrent access and modification during streaming hash computation.
#[derive(Clone)]
pub struct SHA512Hasher(Arc<Mutex<Sha512>>);

impl SHA512Hasher {
    /// Creates a new instance of the SHA-512 hasher.
    ///
    /// # Returns
    ///
    /// A new `SHA512Hasher` instance with an initialized SHA-512 context
    pub fn new() -> Self {
        SHA512Hasher(Arc::new(Mutex::new(Sha512::new())))
    }
}

impl Hasher for SHA512Hasher {
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>> {
        let mut hasher = self.0.lock()?;
        hasher.update(data);
        Ok(())
    }

    fn compute(&self) -> Result<String, Box<dyn Error + '_>> {
        let hasher = self.0.lock()?.clone();
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn to_callback(&self) -> CallbackFun {
        let hasher_arc = self.0.clone();
        CallbackFun::new(Box::new(move |data: &[u8]| {
            let mut hasher = hasher_arc.lock().expect("Failed to lock hasher");
            hasher.update(data);
        }))
    }

    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>> {
        add_checksum(payload, SHA512, self.compute()?)
    }

    fn algorithm(&self) -> &str {
        SHA512
    }
}

/// Implementation of the SHA-1 hash algorithm.
///
/// This struct wraps a SHA-1 context in a thread-safe container to allow for
/// concurrent access and modification during streaming hash computation.
#[derive(Clone)]
pub struct SHA1Hasher(Arc<Mutex<Sha1>>);

impl SHA1Hasher {
    /// Creates a new instance of the SHA-1 hasher.
    ///
    /// # Returns
    ///
    /// A new `SHA1Hasher` instance with an initialized SHA-1 context
    pub fn new() -> Self {
        SHA1Hasher(Arc::new(Mutex::new(Sha1::new())))
    }
}

impl Hasher for SHA1Hasher {
    fn consume(&self, data: &[u8]) -> Result<(), Box<dyn Error + '_>> {
        let mut hasher = self.0.lock()?;
        hasher.update(data);
        Ok(())
    }

    fn compute(&self) -> Result<String, Box<dyn Error + '_>> {
        let hasher = self.0.lock()?.clone();
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn to_callback(&self) -> CallbackFun {
        let hasher_arc = self.0.clone();
        CallbackFun::new(Box::new(move |data: &[u8]| {
            let mut hasher = hasher_arc.lock().expect("Failed to lock hasher");
            hasher.update(data);
        }))
    }

    fn add_to_payload(&self, payload: &mut DirectUploadBody) -> Result<(), Box<dyn Error + '_>> {
        add_checksum(payload, SHA1, self.compute()?)
    }

    fn algorithm(&self) -> &str {
        SHA1
    }
}

/// Adds a checksum to a DirectUploadBody payload.
///
/// This function either updates an existing checksum in the payload or creates a new one
/// with the specified algorithm and hash value.
///
/// # Arguments
///
/// * `payload` - A mutable reference to the `DirectUploadBody` to which the checksum will be added
/// * `algorithm` - A string slice specifying the hash algorithm (e.g., "md5", "sha256")
/// * `hash` - The computed hash value as a string
///
/// # Returns
///
/// A `Result` indicating success or an error if the operation failed
pub(crate) fn add_checksum(
    payload: &mut DirectUploadBody,
    algorithm: &str,
    hash: String,
) -> Result<(), Box<dyn Error>> {
    println!("Adding checksum to payload: {}", hash);
    if let Some(checksum) = payload.checksum.as_mut() {
        checksum.type_ = Some(algorithm.to_string());
        checksum.value = Some(hash);
    } else {
        payload.checksum = Some(Checksum {
            type_: Some(algorithm.to_string()),
            value: Some(hash),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn empty_payload() -> DirectUploadBody {
        DirectUploadBody {
            checksum: None,
            categories: Vec::new(),
            description: None,
            directory_label: None,
            file_name: None,
            mime_type: None,
            restrict: None,
            storage_identifier: None,
        }
    }

    #[test]
    fn test_md5_hasher_callback() {
        // Arrange
        let hasher = MD5Hasher::new();
        let callback = hasher.to_callback();
        let test_string = "Hello, world!";
        let mut payload = empty_payload();

        // Act
        callback.call(&test_string.as_bytes());
        hasher
            .add_to_payload(&mut payload)
            .expect("Failed to add checksum to payload");

        // Assert
        let expected_hash = format!("{:x}", md5::compute(test_string.as_bytes()));
        assert_eq!(payload.checksum.unwrap().value, Some(expected_hash));
    }

    #[test]
    fn test_md5_hasher_consume() {
        // Arrange
        let hasher = MD5Hasher::new();
        let test_string = "Hello, world!";

        // Act
        hasher
            .consume(&test_string.as_bytes())
            .expect("Failed to consume data");

        // Assert
        let expected_hash = format!("{:x}", md5::compute(test_string.as_bytes()));
        let result = hasher.compute().unwrap();
        assert_eq!(result, expected_hash);
    }

    #[test]
    fn test_sha256_hasher_callback() {
        // Arrange
        let hasher = SHA256Hasher::new();
        let callback = hasher.to_callback();
        let test_string = "Hello, world!";
        let mut payload = empty_payload();

        // Act
        callback.call(&test_string.as_bytes());
        hasher
            .add_to_payload(&mut payload)
            .expect("Failed to add checksum to payload");

        // Assert
        let expected_hash = format!("{:x}", sha2::Sha256::digest(test_string.as_bytes()));
        assert_eq!(
            payload.checksum.expect("Checksum not found").value,
            Some(expected_hash)
        );
    }

    #[test]
    fn test_sha256_hasher_consume() {
        // Arrange
        let hasher = SHA256Hasher::new();
        let test_string = "Hello, world!";

        // Act
        hasher
            .consume(&test_string.as_bytes())
            .expect("Failed to consume data");

        // Assert
        let expected_hash = format!("{:x}", sha2::Sha256::digest(test_string.as_bytes()));
        let result = hasher.compute().unwrap();
        assert_eq!(result, expected_hash);
    }

    #[test]
    fn test_sha512_hasher_callback() {
        // Arrange
        let hasher = SHA512Hasher::new();
        let callback = hasher.to_callback();
        let test_string = "Hello, world!";
        let mut payload = empty_payload();

        // Act
        callback.call(&test_string.as_bytes());
        hasher
            .add_to_payload(&mut payload)
            .expect("Failed to add checksum to payload");

        // Assert
        let expected_hash = format!("{:x}", sha2::Sha512::digest(test_string.as_bytes()));
        assert_eq!(
            payload.checksum.expect("Checksum not found").value,
            Some(expected_hash)
        );
    }

    #[test]
    fn test_sha512_hasher_consume() {
        // Arrange
        let hasher = SHA512Hasher::new();
        let test_string = "Hello, world!";

        // Act
        hasher
            .consume(&test_string.as_bytes())
            .expect("Failed to consume data");

        // Assert
        let expected_hash = format!("{:x}", sha2::Sha512::digest(test_string.as_bytes()));
        let result = hasher.compute().unwrap();
        assert_eq!(result, expected_hash);
    }

    #[test]
    fn test_sha1_hasher_callback() {
        // Arrange
        let hasher = SHA1Hasher::new();
        let callback = hasher.to_callback();
        let test_string = "Hello, world!";
        let mut payload = empty_payload();

        // Act
        callback.call(&test_string.as_bytes());
        hasher
            .add_to_payload(&mut payload)
            .expect("Failed to add checksum to payload");

        // Assert
        let expected_hash = format!("{:x}", sha1::Sha1::digest(test_string.as_bytes()));
        assert_eq!(
            payload.checksum.expect("Checksum not found").value,
            Some(expected_hash)
        );
    }

    #[test]
    fn test_sha1_hasher_consume() {
        // Arrange
        let hasher = SHA1Hasher::new();
        let test_string = "Hello, world!";

        // Act
        hasher
            .consume(&test_string.as_bytes())
            .expect("Failed to consume data");

        // Assert
        let expected_hash = format!("{:x}", sha1::Sha1::digest(test_string.as_bytes()));
        let result = hasher.compute().unwrap();
        assert_eq!(result, expected_hash);
    }

    #[test]
    fn test_add_to_payload() {
        // Arrange
        let mut payload = DirectUploadBody {
            checksum: None,
            categories: Vec::new(),
            description: None,
            directory_label: None,
            file_name: None,
            mime_type: None,
            restrict: None,
            storage_identifier: None,
        };

        // Act
        add_checksum(&mut payload, "md5", "Some hash".to_string()).unwrap();

        // Assert
        let checksum = payload.checksum.as_ref().unwrap();
        assert_eq!(checksum.type_, Some("md5".to_string()));
        assert_eq!(checksum.value, Some("Some hash".to_string()));
    }

    #[tokio::test]
    async fn test_file_hash_hash() {
        for algorithm in ["MD5", "SHA-256", "SHA-512", "SHA-1"] {
            // Arrange
            let hasher = FileHash::new(algorithm).unwrap();
            let path = PathBuf::from("tests/fixtures/upload.txt");
            let content = std::fs::read(&path).unwrap();
            let file = UploadFile::from_path(path)
                .await
                .expect("Failed to create upload file");

            // Act
            hasher.hash(file).await.unwrap();

            // Assert
            match algorithm {
                "MD5" => {
                    let expected_hash = format!("{:x}", md5::compute(content));
                    let hash = hasher.compute().unwrap();
                    assert_eq!(hash, expected_hash, "Hash for {} is incorrect", algorithm);
                }
                "SHA-256" => {
                    let expected_hash = format!("{:x}", sha2::Sha256::digest(content));
                    let hash = hasher.compute().unwrap();
                    assert_eq!(hash, expected_hash, "Hash for {} is incorrect", algorithm);
                }
                "SHA-512" => {
                    let expected_hash = format!("{:x}", sha2::Sha512::digest(content));
                    let hash = hasher.compute().unwrap();
                    assert_eq!(hash, expected_hash, "Hash for {} is incorrect", algorithm);
                }
                "SHA-1" => {
                    let expected_hash = format!("{:x}", sha1::Sha1::digest(content));
                    let hash = hasher.compute().unwrap();
                    assert_eq!(hash, expected_hash, "Hash for {} is incorrect", algorithm);
                }
                _ => panic!("Unsupported algorithm: {}", algorithm),
            }
        }
    }
}
