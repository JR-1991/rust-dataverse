use std::path::PathBuf;

/// Infers the MIME type of a file from its extension or content.
///
/// This function attempts to determine the MIME type of a file using two methods:
/// 1. First, it tries to infer the MIME type from the file extension using `mime_guess`.
/// 2. If that fails, it tries to infer the MIME type from the file's magic number using `infer`.
///
/// # Arguments
///
/// * `path` - A reference to the path of the file to infer the MIME type from.
///
/// # Returns
///
/// * `Ok(String)` - The inferred MIME type as a string.
/// * `Err(String)` - An error message if the MIME type could not be inferred.
pub fn infer_mime(path: &PathBuf) -> Result<String, String> {
    // First try to infer the mime type from the file extension
    if let Some(mime) = guess_mime(path) {
        Ok(mime)
    } else if let Some(mime) = infer_from_magic_number(path) {
        Ok(mime)
    } else {
        Err("Failed to infer mime type".to_string())
    }
}

/// Attempts to guess the MIME type of a file from its extension.
///
/// # Arguments
///
/// * `path` - A reference to the path of the file.
///
/// # Returns
///
/// * `Some(String)` - The guessed MIME type as a string.
/// * `None` - If the MIME type could not be guessed from the extension.
fn guess_mime(path: &PathBuf) -> Option<String> {
    let mime = mime_guess::from_path(path).first();
    mime.map(|m| m.to_string())
}

/// Attempts to infer the MIME type of a file from its magic number.
///
/// This function reads the beginning of the file to identify its type based on
/// file signatures (magic numbers).
///
/// # Arguments
///
/// * `path` - A reference to the path of the file.
///
/// # Returns
///
/// * `Some(String)` - The inferred MIME type as a string.
/// * `None` - If the MIME type could not be inferred from the file content.
fn infer_from_magic_number(path: &PathBuf) -> Option<String> {
    // Use infer::get_from_path which handles file opening and reading
    match infer::get_from_path(path) {
        Ok(Some(kind)) => Some(kind.mime_type().to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_mime_from_extension() {
        let path = PathBuf::from("tests/fixtures/file.txt");
        let mime = infer_mime(&path).unwrap();
        assert_eq!(mime, "text/plain");
    }

    #[test]
    fn test_infer_mime_from_magic_number() {
        let path = PathBuf::from("tests/fixtures/image");
        let mime = infer_mime(&path).unwrap();
        assert_eq!(mime, "image/png");
    }

    #[test]
    #[should_panic]
    fn test_infer_mime_from_unknown_file() {
        let path = PathBuf::from("tests/fixtures/unknown");
        infer_mime(&path).expect("Failed to infer mime type");
    }
}
