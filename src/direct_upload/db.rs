use std::path::Path;

use sqlx::{migrate::MigrateDatabase, Sqlite};

use super::model;

// We are creating the database in the user's home directory
// This way the database is portable across different machines
lazy_static::lazy_static! {
    static ref DB_URL: String = get_db_path().expect("Failed to get db path");
}

/// Create the database if it doesn't exist and return the database connection pool
///
/// # Arguments
///
/// * `path` - The path to the database file
///
/// # Returns
///
/// * `Result<sqlx::Pool<Sqlite>, sqlx::Error>` - The database connection pool
pub(crate) async fn create_database(
    path: Option<String>,
) -> Result<sqlx::Pool<Sqlite>, sqlx::Error> {
    let db_path = path.unwrap_or(DB_URL.clone());
    if !Sqlite::database_exists(&db_path).await.unwrap_or(false) {
        // Create missing directories
        let dir = Path::new(db_path.as_str()).parent().unwrap();
        std::fs::create_dir_all(dir).unwrap();

        // Create the database
        match Sqlite::create_database(&db_path).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    }

    // Connect to the database and create tables
    let pool = sqlx::SqlitePool::connect(&db_path).await?;

    // Create tables
    model::Part::create_table(&pool).await?;
    model::Upload::create_table(&pool).await?;

    Ok(pool)
}

/// Get the path to the database
///
/// This function is lazy initialized and will only be called once
///
/// Returns the path to the database
///
/// # Panics
///
/// If the home directory is not found
fn get_db_path() -> Result<String, String> {
    let home_dir = dirs::home_dir().ok_or("Home directory not found")?;
    Ok(home_dir
        .to_str()
        .ok_or("Home directory not found")?
        .to_string()
        + "/.config/dvcli/db.sqlite")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_database() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_path = tmp_dir.path().join("db.sqlite");
        create_database(Some(db_path.to_str().unwrap().to_string()))
            .await
            .expect("Failed to create database");

        // Check if the database exists
        assert!(Sqlite::database_exists(&db_path.to_str().unwrap())
            .await
            .expect("Failed to check if database exists"));
    }
}
