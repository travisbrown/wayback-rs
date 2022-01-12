use crate::{
    util::sqlite::{SQLiteEpochSecond, SQLiteId},
    Item,
};
use chrono::naive::NaiveDateTime;
use futures_locks::RwLock;
use rusqlite::{params, CachedStatement, Connection, DropBehavior, OptionalExtension, Transaction};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::Path;

pub struct Store {
    connection: RwLock<Connection>,
    mime_types: HashMap<String, u64>,
}

impl Store {
    fn load_schema() -> std::io::Result<String> {
        std::fs::read_to_string("schemas/item.sql")
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Store, rusqlite::Error> {
        let exists = path.as_ref().is_file();
        let connection = Connection::open(path)?;

        if !exists {
            let schema = Self::load_schema().expect("Item index schema not available");
            connection.execute_batch(&schema)?;
        }

        Ok(Store {
            connection: RwLock::new(connection),
            mime_types: HashMap::new(),
        })
    }

    pub async fn add_items<'a, I: IntoIterator<Item = Item>>(
        &'a mut self,
        items: I,
    ) -> Result<AddOperationStats, rusqlite::Error> {
        let mut connection = self.connection.write().await;
        let mut tx = connection.transaction()?;
        tx.set_drop_behavior(DropBehavior::Commit);

        let mut url_select = tx.prepare_cached(URL_SELECT)?;
        let mut url_insert = tx.prepare_cached(URL_INSERT)?;

        let mut digest_select = tx.prepare_cached(DIGEST_SELECT)?;
        let mut digest_insert = tx.prepare_cached(DIGEST_INSERT)?;

        let mut mime_type_select = tx.prepare_cached(MIME_TYPE_SELECT)?;
        let mut mime_type_insert = tx.prepare_cached(MIME_TYPE_INSERT)?;

        let mut item_select = tx.prepare_cached(ITEM_SELECT)?;
        let mut item_insert = tx.prepare_cached(ITEM_INSERT)?;

        let mut skip_count = 0;
        let mut write_count = 0;
        let mut overwrite_count = 0;
        let mut ignore_count = 0;
        let mut collisions = HashSet::new();

        for item in items {
            let url_id = get_or_add(&tx, &mut url_select, &mut url_insert, &item.url)?;
            let digest_id = get_or_add(&tx, &mut digest_select, &mut digest_insert, &item.digest)?;

            let mime_type_id = self.get_mime_type_id(
                &tx,
                &mut mime_type_select,
                &mut mime_type_insert,
                &item.mime_type,
            )?;

            let should_add = match self.check_existing(
                &tx,
                &mut item_select,
                &item,
                url_id,
                digest_id,
                mime_type_id,
            )? {
                OnExisting::Skip => {
                    skip_count += 1;
                    false
                }
                OnExisting::Write => {
                    write_count += 1;
                    true
                }
                OnExisting::Overwrite => {
                    overwrite_count += 1;
                    true
                }
                OnExisting::Ignore => {
                    ignore_count += 1;
                    false
                }
                OnExisting::Collision { id } => {
                    collisions.insert((id, item.clone()));
                    false
                }
            };

            if should_add {
                item_insert.execute(params![
                    url_id,
                    SQLiteEpochSecond(item.archived_at),
                    digest_id,
                    mime_type_id,
                    item.length,
                    item.status
                ])?;
            }
        }

        Ok(AddOperationStats {
            skip_count,
            write_count,
            overwrite_count,
            ignore_count,
            collisions,
        })
    }

    fn check_existing(
        &self,
        tx: &Transaction,
        select: &mut CachedStatement,
        item: &Item,
        item_url_id: u64,
        item_digest_id: u64,
        item_mime_type_id: u64,
    ) -> Result<OnExisting, rusqlite::Error> {
        select
            .query_row(
                params![
                    item_url_id,
                    SQLiteEpochSecond(item.archived_at),
                    item_digest_id
                ],
                |row| {
                    let id = row.get::<usize, i64>(0)? as u64;
                    let mime_type_id = row.get::<usize, i64>(4)? as u64;
                    let length: u32 = row.get(5)?;
                    let status: Option<u16> = row.get(6)?;

                    if mime_type_id != item_mime_type_id {
                        if mime_type_id == WARC_REVISIT_ID {
                            // If the new MIME type is a revisit, we ignore it
                            Ok(OnExisting::Ignore)
                        } else if item_mime_type_id == WARC_REVISIT_ID {
                            // If the old MIME type is a revisit, we overwrite it
                            Ok(OnExisting::Overwrite)
                        } else {
                            // Otherwise we don't know what to do
                            Ok(OnExisting::Collision { id })
                        }
                    } else if status != item.status {
                        Ok(OnExisting::Collision { id })
                    } else {
                        match length.cmp(&item.length) {
                            Ordering::Equal => Ok(OnExisting::Skip),
                            Ordering::Less => Ok(OnExisting::Ignore),
                            Ordering::Greater => Ok(OnExisting::Overwrite),
                        }
                    }
                },
            )
            .optional()
            .map(|maybe_value| maybe_value.unwrap_or(OnExisting::Write))
    }

    fn get_mime_type_id(
        &mut self,
        tx: &Transaction,
        select: &mut CachedStatement,
        insert: &mut CachedStatement,
        value: &str,
    ) -> Result<u64, rusqlite::Error> {
        let id = match self.mime_types.get(value) {
            Some(id) => *id,
            None => {
                let id = get_or_add(tx, select, insert, value)?;
                self.mime_types.insert(value.to_string(), id);
                id
            }
        };

        Ok(id)
    }
}

fn get_or_add(
    tx: &Transaction,
    select: &mut CachedStatement,
    insert: &mut CachedStatement,
    value: &str,
) -> Result<u64, rusqlite::Error> {
    let ps = params![value];
    match select
        .query_row(ps, |row| row.get::<usize, i64>(0))
        .optional()?
    {
        Some(id) => Ok(id as u64),
        None => {
            insert.execute(ps)?;
            Ok(tx.last_insert_rowid() as u64)
        }
    }
}

enum OnExisting {
    /// The item is already in the database
    Skip,
    /// There is no item in the database with this URL and timestamp
    Write,
    /// There is an item in the database with this URL and timestamp but we want to replace it
    Overwrite,
    /// There is an item in the database with this URL and timestamp and we don't want to replace it
    Ignore,
    /// There is an item in the database with this URL and timestamp and we don't know what to do
    Collision { id: u64 },
}

#[derive(Debug)]
pub struct AddOperationStats {
    skip_count: usize,
    write_count: usize,
    overwrite_count: usize,
    ignore_count: usize,
    collisions: HashSet<(u64, Item)>,
}

const WARC_REVISIT_ID: u64 = 1;

const URL_SELECT: &str = "SELECT id FROM url WHERE value = ?";
const URL_INSERT: &str = "INSERT INTO url (value) VALUES (?)";

const DIGEST_SELECT: &str = "SELECT id FROM digest WHERE value = ?";
const DIGEST_INSERT: &str = "INSERT INTO digest (value) VALUES (?)";

const MIME_TYPE_SELECT: &str = "SELECT id FROM mime_type WHERE value = ?";
const MIME_TYPE_INSERT: &str = "INSERT INTO mime_type (value) VALUES (?)";

const ITEM_SELECT: &str = "
    SELECT id, url_id, timestamp_s, digest_id, mime_type_id, length, status FROM item
        WHERE url_id = ? AND timestamp_s = ? AND digest_id = ?
";

const ITEM_INSERT: &str = "
    INSERT INTO item (url_id, timestamp_s, digest_id, mime_type_id, length, status) VALUES (?, ?, ?, ?, ?, ?)
";
