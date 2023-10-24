// use scylla::Session;
// 
// #[tokio::main]
// async fn main() {
//     println!("Hello scylla!");
// }

use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a new Session which connects to node at 127.0.0.1:9042
    // (or SCYLLA_URI if specified)
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await?;

    // Create an example keyspace and table
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.extab (a int primary key)",
            &[],
        )
        .await?;


    // // Insert a value into the table
    // let to_insert: i32 = 12345;
    // session
    //     .query("INSERT INTO ks.extab (a) VALUES(?)", (to_insert,))
    //     .await?;

    // // Query rows from the table and print them
    // if let Some(rows) = session.query("SELECT a FROM ks.extab", &[]).await?.rows {
    //     // Parse each row as a tuple containing single i32
    //     for row in rows.into_typed::<(i32,)>() {
    //         let read_row: (i32,) = row?;
    //         println!("Read a value from row: {}", read_row.0);
    //     }
    // }

    let to_insert2 : i32 = 65810;
    session
        .query("INSERT INTO ks.extab(a) VALUES(?)", (to_insert2,))
        .await?;

    if let Some(rows) = session.query("SELECT a FROM ks.extab", &[]).await?.rows {
        // Parse each row as a tuple containing single i32
        for row in rows.into_typed::<(i32,)>() {
            let read_row: (i32,) = row?;
            println!("Read a value from row: {}", read_row.0);
        }
    }

    Ok(())
}
