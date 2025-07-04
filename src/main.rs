use jwalk::WalkDir;
use rusqlite::{Connection, Result, params};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug)]
struct Datei<'a> {
    hash: u64,
    path: &'a str,
    size: i64,
    created: i64,
    modified: i64,
    plen: i64,
    flen: i64,
    timestamp: i64,
}

fn create_database(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = 100000;
        PRAGMA temp_store = MEMORY;
        ",
    )?;

    connection.execute(
        "CREATE TABLE IF NOT EXISTS files (
            hash TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            plen INTEGER NOT NULL,
            flen INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            new INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (hash, path)
        );",
        (),
    )?;

    Ok(())
}

fn table_is_empty(connection: &Connection, table: &str) -> Result<bool> {
    Ok(connection.query_row::<i64, _, _>(
        &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)", table),
        [],
        |row| row.get(0),
    )? == 0)
}

fn create_index(connection: &Connection) {
    connection
        .execute("CREATE INDEX IF NOT EXISTS idx_hash ON files(hash)", ())
        .expect("INDEX ERROR ON HASH");
    connection
        .execute("CREATE INDEX IF NOT EXISTS idx_path ON files(path)", ())
        .expect("INDEX ERROR ON PATH");
    connection
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON files(timestamp)",
            (),
        )
        .expect("INDEX ERROR ON TIMESTAMP");
    connection
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_last_seen ON files(last_seen)",
            (),
        )
        .expect("INDEX ERROR ON LAST_SEEN");
    connection
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_new ON files(new)",
            (),
        )
        .expect("INDEX ERROR ON NEW");
}

fn write_to_file(connection: &Connection, path: &str) {

}

fn main() -> Result<()> {
    let mut db = Connection::open("files.db")?;
    create_database(&db)?;

    {
        let tx = db.transaction()?;

        let mut insert = tx.prepare_cached(
            "INSERT INTO files (hash, path, size, created, modified, plen, flen, timestamp, last_seen, new) 
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) 
            ON CONFLICT(hash, path) DO UPDATE SET
                size = excluded.size,
                created = excluded.created,
                modified = excluded.modified,
                plen = excluded.plen,
                flen = excluded.flen,
                last_seen = excluded.last_seen,
                timestamp = CASE
                                WHEN size <> excluded.size 
                                OR modified <> excluded.modified
                                THEN excluded.timestamp
                                ELSE timestamp
                            END,
                new = 0"
        )?;

        tx.execute("UPDATE files SET new = 0", ())?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for dir_entry in WalkDir::new("/home/simon") {
            match dir_entry {
                Ok(entry) => {
                    if !entry.file_type().is_file() {
                        continue;
                    }

                    let path = entry.path();

                    let hash = xxh3_64(path.to_str().unwrap().as_bytes());
                    let size = entry.metadata().unwrap().len();
                    let created = entry
                        .metadata()
                        .unwrap()
                        .created()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let modified = entry
                        .metadata()
                        .unwrap()
                        .modified()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let plen = path.to_str().unwrap().len();
                    let flen = entry.file_name.len();

                    insert.execute(params![
                        hash.to_string(),
                        path.to_str(),
                        size,
                        created,
                        modified,
                        plen,
                        flen,
                        timestamp,
                        timestamp,
                        1
                    ])?;
                }
                Err(_) => (),
            };
        }
        drop(insert);
        tx.commit()?;

        let mut query_new = db.prepare("SELECT path, size, created, modified,  FROM files WHERE new = 1;")?;
        let mut query_modified = db.prepare("SELECT path FROM files WHERE timestamp = ?1 AND new = 0")?;
        let mut query_deleted = db.prepare("SELECT path FROM files WHERE last_seen <> ?1")?;
        
        db.execute("DELETE FROM files WHERE last_seen <> ?1", [timestamp])?;
    }

    create_index(&db);

    Ok(())
}
