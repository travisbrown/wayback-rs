use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::Result;

pub struct SQLiteId(pub u64);

impl ToSql for SQLiteId {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0 as i64)))
    }
}

pub struct SQLiteDateTime(pub DateTime<Utc>);

impl ToSql for SQLiteDateTime {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(
            self.0.timestamp_millis(),
        )))
    }
}

impl FromSql for SQLiteDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ts: i64 = FromSql::column_result(value)?;

        Ok(SQLiteDateTime(Utc.timestamp_millis(ts)))
    }
}

pub struct SQLiteNaiveDateTime(pub NaiveDateTime);

impl ToSql for SQLiteNaiveDateTime {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(
            self.0.timestamp_millis(),
        )))
    }
}

impl FromSql for SQLiteNaiveDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ts: i64 = FromSql::column_result(value)?;

        Ok(SQLiteNaiveDateTime(Utc.timestamp_millis(ts).naive_utc()))
    }
}
