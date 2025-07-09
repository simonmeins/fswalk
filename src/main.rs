use jwalk::WalkDir;
use rusqlite::{params, Connection, Result, Row, Rows};
use std::{
    fs::File,
    io::{BufWriter, Write},
    ops::Deref,
};
use tabled::{
    Table, Tabled,
    builder::Builder,
    settings::Style
};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Tabled)]
struct Datei {
    path: String,
    size: i64,
    created: i64,
    modified: i64,
    plen: i64,
    flen: i64,
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
        .execute("CREATE INDEX IF NOT EXISTS idx_new ON files(new)", ())
        .expect("INDEX ERROR ON NEW");
}

fn create_file(path: &str) -> std::io::Result<BufWriter<File>> {
    let file = File::create(path)?;
    Ok(BufWriter::with_capacity(32 * 1024 * 1024, file))
}

fn process_row(row: &Row) -> Datei {
    Datei {
        path: row.get_unwrap::<_, String>("path"),
        size: row.get_unwrap::<_, i64>("size"),
        created: row.get_unwrap::<_, i64>("created"),
        modified: row.get_unwrap::<_, i64>("modified"),
        plen: row.get_unwrap::<_, i64>("plen"),
        flen: row.get_unwrap::<_, i64>("flen"),
    }
}

fn build_table(mut rows: Rows) -> Result<Table> {
    let mut table_builder = Builder::default();
    table_builder.push_record(vec!["PATH", "SIZE", "CREATED", "MODIFIED", "PLEN", "FLEN"]);

    while let Some(row) = rows.next()? {
        let datei = process_row(&row);
        table_builder.push_record(vec![
            datei.path,
            datei.size.to_string(),
            datei.created.to_string(),
            datei.modified.to_string(),
            datei.plen.to_string(),
            datei.flen.to_string()
        ]);
    }

    let mut table = table_builder.build();
    table.with(Style::psql());

    Ok(table)
}

fn write_to_file(connection: &mut Connection, path: &str, timestamp: u64) -> Result<()> {
    let tx = connection.transaction()?;

    {
        let mut sql_query_new_count = tx.prepare("SELECT COUNT(*) from files WHERE new = 1;")?;
        let mut sql_query_modified_count =
            tx.prepare("SELECT COUNT(*) FROM files WHERE timestamp = ?1 AND new = 0;")?;
        let mut sql_query_deleted_count =
            tx.prepare("SELECT COUNT(*) FROM files WHERE last_seen <> ?1")?;

        let mut sql_query_new = tx.prepare(
            "SELECT hash, path, size, created, modified, plen, flen FROM files WHERE new = 1;",
        )?;
        let mut sql_query_modified = tx.prepare("SELECT hash, path, size, created, modified, plen, flen FROM files WHERE timestamp = ?1 AND new = 0;")?;
        let mut sql_query_deleted = tx.prepare("SELECT hash, path, size, created, modified, plen, flen FROM files WHERE last_seen <> ?1;")?;

        let mut file = create_file("output.txt").expect("Error creating file");

        let query_rows_new = sql_query_new.query([])?;
        let query_rows_modified = sql_query_modified.query([timestamp])?;
        let query_rows_deleted = sql_query_deleted.query([timestamp])?;

        let t = build_table(query_rows_new)?;
        let t1 = build_table(query_rows_modified)?;
        let t2 = build_table(query_rows_deleted)?;

        println!("{}", t1);

        /*let new_rows = query_new.query_map([], |row| {
            Ok(Datei {
                path: row.get::<_, String>("path")?,
                size: row.get::<_, i64>("size")?,
                created: row.get::<_, i64>("created")?,
                modified: row.get::<_, i64>("modified")?,
                plen: row.get::<_, i64>("plen")?,
                flen: row.get::<_, i64>("flen")?
            })
        })?;

        let mut table = Table::new(new_rows.map(Result::unwrap));
        table
            .with(Style::psql())
            .with(Modify::new(Rows::first()).with(Format::content(|s| s.to_uppercase())));*/

        //writeln!(file, "{}", table).expect("ERROR");

        file.flush().expect("Writer flush error");
    }
    tx.commit()?;

    Ok(())
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

        for dir_entry in WalkDir::new("/home/simon/Documents/Hallo") {
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

        write_to_file(&mut db, "output.txt", timestamp)?;

        db.execute("DELETE FROM files WHERE last_seen <> ?1", [timestamp])?;
    }

    create_index(&db);

    Ok(())
}
