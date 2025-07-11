use chrono::{Local, TimeZone};
use jwalk::{DirEntry, WalkDir};
use rusqlite::{Connection, Result, Row, Rows, params};
use std::{
    fs::File,
    io::{BufWriter, Write},
};
use tabled::{Table, builder::Builder, settings::Style};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug)]
struct Datei {
    hash: String,
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
    let now = Local::now();

    let filename = format!("Krose_{}.txt", now.format("%Y%m%d_%H%M%S"));

    let file = File::create(filename)?;
    Ok(BufWriter::with_capacity(32 * 1024 * 1024, file))
}

fn process_row(row: &Row) -> Datei {
    Datei {
        hash: row.get_unwrap::<_, String>("hash"),
        path: row.get_unwrap::<_, String>("path"),
        size: row.get_unwrap::<_, i64>("size"),
        created: row.get_unwrap::<_, i64>("created"),
        modified: row.get_unwrap::<_, i64>("modified"),
        plen: row.get_unwrap::<_, i64>("plen"),
        flen: row.get_unwrap::<_, i64>("flen"),
    }
}

fn build_table(mut rows: Rows) -> Result<Option<Table>> {
    let mut table_builder = Builder::default();
    table_builder.push_record(vec!["SIZE", "CREATED", "MODIFIED", "PLEN", "FLEN", "PATH"]);

    let mut found = false;

    while let Some(row) = rows.next()? {
        found = true;
        let datei = process_row(&row);

        let created = Local
            .timestamp_opt(datei.created, 0)
            .unwrap()
            .format("%d.%m.%Y %H:%M:%S")
            .to_string();
        let modified = Local
            .timestamp_opt(datei.modified, 0)
            .unwrap()
            .format("%d.%m.%Y %H:%M:%S")
            .to_string();

        table_builder.push_record(vec![
            datei.size.to_string(),
            created,
            modified,
            datei.plen.to_string(),
            datei.flen.to_string(),
            datei.path,
        ]);
    }

    if !found {
        return Ok(None);
    }

    let mut table = table_builder.build();
    table.with(Style::psql());

    Ok(Some(table))
}

fn write_to_file(connection: &mut Connection, path: &str, timestamp: u64) -> Result<()> {
    let tx = connection.transaction()?;

    {
        /*let mut sql_query_new_count = tx.prepare("SELECT COUNT(*) from files WHERE new = 1;")?;
        let mut sql_query_modified_count =
            tx.prepare("SELECT COUNT(*) FROM files WHERE timestamp = ?1 AND new = 0;")?;
        let mut sql_query_deleted_count =
            tx.prepare("SELECT COUNT(*) FROM files WHERE last_seen <> ?1")?;*/

        let mut sql_query_total_files_space =
            tx.prepare("SELECT round(SUM(size) / 1000000000.0, 2) FROM files;")?;
        let mut sql_query_total_files_count = tx.prepare("SELECT COUNT(*) FROM files;")?;
        let mut sql_query_new = tx.prepare(
            "SELECT hash, path, size, created, modified, plen, flen FROM files WHERE new = 1;",
        )?;
        let mut sql_query_modified = tx.prepare("SELECT hash, path, size, created, modified, plen, flen FROM files WHERE timestamp = ?1 AND new = 0;")?;
        let mut sql_query_deleted = tx.prepare("SELECT hash, path, size, created, modified, plen, flen FROM files WHERE last_seen <> ?1;")?;

        let mut file = create_file("output.txt").expect("Error creating file");

        let query_total_files_space =
            sql_query_total_files_space.query_one([], |row| Ok(row.get::<_, f64>(0)?))?;
        let query_total_files_count =
            sql_query_total_files_count.query_one([], |row| Ok(row.get::<_, i64>(0)?))?;
        let query_rows_new = sql_query_new.query([])?;
        let query_rows_modified = sql_query_modified.query([timestamp])?;
        let query_rows_deleted = sql_query_deleted.query([timestamp])?;

        let table_new_files = build_table(query_rows_new)?;
        let table_modified_files = build_table(query_rows_modified)?;
        let table_deleted_files = build_table(query_rows_deleted)?;

        writeln!(
            file,
            "Anzahl Dateien: {}\nSpeicherplatz belegt: {} GB\n\n",
            query_total_files_count, query_total_files_space
        )
        .expect("Error while writing total files count");

        if let Some(table_new) = table_new_files {
            writeln!(file, "Neue Dateien:\n\n{}\n\n", table_new)
                .expect("Error while writing new files to file");
        }

        if let Some(table_modified) = table_modified_files {
            writeln!(file, "Geänderte Dateien:\n\n{}\n\n", table_modified)
                .expect("Error while writing modified files to file");
        }

        if let Some(table_deleted) = table_deleted_files {
            writeln!(file, "Gelöschte Dateien:\n\n{}", table_deleted)
                .expect("Error while writing deleted files to file");
        }

        file.flush().expect("Writer flush error");
    }
    tx.commit()?;

    Ok(())
}

fn process_dir_entry(entry: &DirEntry<((), ())>) -> Result<Datei> {
    let metadata = entry.metadata();
    let path = entry.path();

    let hash = xxh3_64(path.to_str().unwrap().as_bytes()).to_string();

    let size = match metadata {
        Ok(ref data) => data.len(),
        Err(_) => 0,
    } as i64;

    let created = match metadata {
        Ok(ref data) => match data.created() {
            Ok(time) => time
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Err(_) => 0,
        },
        Err(_) => 0,
    } as i64;

    let modified = match metadata {
        Ok(ref data) => match data.modified() {
            Ok(time) => time
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Err(_) => 0,
        },
        Err(_) => 0,
    } as i64;

    let plen = path.to_str().unwrap().len() as i64;
    let flen = entry.file_name.len() as i64;

    Ok(Datei {
        hash: hash,
        path: path.to_str().unwrap().to_string(),
        size: size,
        created: created,
        modified: modified,
        plen: plen,
        flen: flen,
    })
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

        for dir_entry in WalkDir::new("/home/simon/") {
            match dir_entry {
                Ok(entry) => {
                    if !entry.file_type().is_file() {
                        continue;
                    }

                    let datei = process_dir_entry(&entry)?;

                    insert.execute(params![
                        datei.hash,
                        datei.path,
                        datei.size,
                        datei.created,
                        datei.modified,
                        datei.plen,
                        datei.flen,
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
