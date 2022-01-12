use chrono::naive::NaiveDateTime;
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::Result;

pub(crate) struct SQLiteId(pub(crate) u64);

impl ToSql for SQLiteId {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0 as i64)))
    }
}

pub(crate) struct SQLiteEpochSecond(pub(crate) NaiveDateTime);

impl ToSql for SQLiteEpochSecond {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0.timestamp())))
    }
}

impl FromSql for SQLiteEpochSecond {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ts: i64 = FromSql::column_result(value)?;

        Ok(Self(NaiveDateTime::from_timestamp(ts, 0)))
    }
}
