use crate::{util::sqlite::SQLiteNaiveDateTime, Item};
use futures_locks::RwLock;
use rusqlite::{params, CachedStatement, Connection, DropBehavior, OptionalExtension, Transaction};
use std::path::Path;
use thiserror::Error;

pub type ItemStoreResult<T> = Result<T, ItemStoreError>;

#[derive(Error, Debug)]
pub enum ItemStoreError {
    #[error("Missing file for ItemStore")]
    FileMissing(#[from] std::io::Error),
    #[error("SQLite error for ItemStore")]
    DbFailure(#[from] rusqlite::Error),
}

#[derive(Clone)]
pub struct ItemStore {
    connection: RwLock<Connection>,
}

impl ItemStore {
    fn load_schema() -> std::io::Result<String> {
        std::fs::read_to_string("schemas/item.sql")
    }

    pub fn new<P: AsRef<Path>>(path: P, recreate: bool) -> ItemStoreResult<ItemStore> {
        let exists = path.as_ref().is_file();
        let mut connection = Connection::open(path)?;

        if exists {
            if recreate {
                let tx = connection.transaction()?;
                tx.execute("DROP TABLE IF EXISTS url", [])?;
                tx.execute("DROP TABLE IF EXISTS digest", [])?;
                tx.execute("DROP TABLE IF EXISTS mime_type", [])?;
                tx.execute("DROP TABLE IF EXISTS item", [])?;
                tx.execute("DROP TABLE IF EXISTS size", [])?;
                let schema = Self::load_schema()?;
                tx.execute_batch(&schema)?;
                tx.commit()?;
            }
        } else {
            let schema = Self::load_schema()?;
            connection.execute_batch(&schema)?;
        }

        Ok(ItemStore {
            connection: RwLock::new(connection),
        })
    }

    pub async fn add_items<'a, I: Iterator<Item = Item>>(
        &'a self,
        items: I,
    ) -> ItemStoreResult<()> {
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

        let mut size_insert = tx.prepare_cached(SIZE_INSERT)?;

        for item in items {
            let url_id_res = get_or_add(&tx, &mut url_select, &mut url_insert, &item.url)?;
            let digest_id_res =
                get_or_add(&tx, &mut digest_select, &mut digest_insert, &item.digest)?;
            let mime_type_id_res = get_or_add(
                &tx,
                &mut mime_type_select,
                &mut mime_type_insert,
                &item.mime_type,
            )?;

            let any_inserts =
                url_id_res.is_err() || digest_id_res.is_err() || mime_type_id_res.is_err();

            let url_id = merge(url_id_res);
            let digest_id = merge(digest_id_res);
            let mime_type_id = merge(mime_type_id_res);

            let item_params = params![
                url_id,
                SQLiteNaiveDateTime(item.archived_at),
                digest_id,
                mime_type_id,
                item.status
            ];

            let existing_item_id = if any_inserts {
                None
            } else {
                item_select
                    .query_row(item_params, |row| row.get::<usize, i64>(0))
                    .optional()?
            };

            let item_id = match existing_item_id {
                Some(id) => id,
                None => {
                    item_insert.execute(item_params)?;
                    tx.last_insert_rowid()
                }
            };

            if item.length != 0 {
                size_insert.execute(params![item_id, item.length])?;
            }
        }

        Ok(())
    }

    pub async fn for_each_item<F: FnMut(Item) -> ()>(&self, mut f: F) -> ItemStoreResult<()> {
        let connection = self.connection.write().await;
        let mut select = connection.prepare(ITEM_LIST)?;

        let items = select.query_and_then(params![], |row| {
            let url = row.get(0)?;
            let archived_at: SQLiteNaiveDateTime = row.get(1)?;
            let digest = row.get(2)?;
            let mime_type = row.get(3)?;
            let length = row.get(4)?;
            let status = row.get(5)?;

            let result: ItemStoreResult<Item> = Ok(Item::new(
                url,
                archived_at.0,
                digest,
                mime_type,
                length,
                status,
            ));

            result
        })?;

        for item in items {
            f(item?);
        }

        Ok(())
    }
}

fn merge<T>(result: Result<T, T>) -> T {
    match result {
        Ok(value) => value,
        Err(value) => value,
    }
}

fn get_or_add(
    tx: &Transaction,
    select: &mut CachedStatement,
    insert: &mut CachedStatement,
    value: &str,
) -> ItemStoreResult<Result<i64, i64>> {
    let ps = params![value];
    match select
        .query_row(ps, |row| row.get::<usize, i64>(0))
        .optional()?
    {
        Some(id) => Ok(Ok(id)),
        None => {
            insert.execute(ps)?;
            Ok(Err(tx.last_insert_rowid()))
        }
    }
}

const URL_SELECT: &str = "SELECT id FROM url WHERE value = ?";
const URL_INSERT: &str = "INSERT INTO url (value) VALUES (?)";

const DIGEST_SELECT: &str = "SELECT id FROM digest WHERE value = ?";
const DIGEST_INSERT: &str = "INSERT INTO digest (value) VALUES (?)";

const MIME_TYPE_SELECT: &str = "SELECT id FROM mime_type WHERE value = ?";
const MIME_TYPE_INSERT: &str = "INSERT INTO mime_type (value) VALUES (?)";

const SIZE_INSERT: &str = "INSERT OR IGNORE INTO size (item_id, value) VALUES (?, ?)";

const ITEM_SELECT: &str = "
    SELECT id FROM item
        WHERE url_id = ? AND ts = ? AND digest_id = ? AND mime_type_id = ? AND status IS ?
";
const ITEM_INSERT: &str = "
    INSERT INTO item (url_id, ts, digest_id, mime_type_id, status) VALUES (?, ?, ?, ?, ?)
";

const ITEM_LIST: &str = "
    SELECT url.value, item.ts, digest.value, mime_type.value, size.value, item.status
        FROM item
        JOIN url ON url.id = item.url_id
        JOIN digest ON digest.id = item.digest_id
        JOIN mime_type ON mime_type.id = item.mime_type_id
        JOIN size ON size.item_id = item.id
";
