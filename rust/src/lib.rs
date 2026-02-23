use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use serde_json::{json, Map, Value};

#[cfg(feature = "native")]
mod native {
    use super::*;
    use vo_ext::prelude::*;
    use vo_runtime::builtins::error_helper::{write_error_to, write_nil_error};

    lazy_static! {
        static ref DBS: Mutex<HashMap<u32, Connection>> = Mutex::new(HashMap::new());
    }

    static NEXT_ID: AtomicU32 = AtomicU32::new(1);

    fn get_db<'a>(dbs: &'a HashMap<u32, Connection>, id: u32) -> Result<&'a Connection, String> {
        dbs.get(&id).ok_or_else(|| format!("invalid db id {}", id))
    }

    fn get_db_mut<'a>(
        dbs: &'a mut HashMap<u32, Connection>,
        id: u32,
    ) -> Result<&'a mut Connection, String> {
        dbs.get_mut(&id)
            .ok_or_else(|| format!("invalid db id {}", id))
    }

    fn json_row(col_names: &[String], row: &rusqlite::Row<'_>) -> Result<Value, String> {
        let mut obj = Map::new();
        for (idx, name) in col_names.iter().enumerate() {
            let val = row.get_ref(idx).map_err(|e| e.to_string())?;
            obj.insert(name.clone(), value_ref_to_json(val));
        }
        Ok(Value::Object(obj))
    }

    fn value_ref_to_json(v: ValueRef<'_>) -> Value {
        match v {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => json!(i),
            ValueRef::Real(f) => json!(f),
            ValueRef::Text(t) => json!(String::from_utf8_lossy(t).to_string()),
            ValueRef::Blob(b) => json!(b),
        }
    }

    fn open_impl(path: &str) -> Result<u32, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.insert(id, conn);
        Ok(id)
    }

    fn open_in_memory_impl() -> Result<u32, String> {
        let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.insert(id, conn);
        Ok(id)
    }

    fn close_impl(id: u64) -> Result<(), String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.remove(&id)
            .ok_or_else(|| format!("invalid db id {}", id))?;
        Ok(())
    }

    fn exec_impl(id: u64, sql: &str) -> Result<(), String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db_mut(&mut dbs, id)?;
        db.execute_batch(sql).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn execute_impl(id: u64, sql: &str) -> Result<u64, String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db_mut(&mut dbs, id)?;
        let rows_affected = db.execute(sql, []).map_err(|e| e.to_string())?;
        Ok(rows_affected as u64)
    }

    fn query_impl(id: u64, sql: &str) -> Result<Vec<u8>, String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, id)?;

        let mut stmt = db.prepare(sql).map_err(|e| e.to_string())?;
        let col_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|n| (*n).to_string())
            .collect();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out: Vec<Value> = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(json_row(&col_names, row)?);
        }

        serde_json::to_vec(&json!({ "rows": out })).map_err(|e| e.to_string())
    }

    fn query_one_impl(id: u64, sql: &str) -> Result<Vec<u8>, String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, id)?;

        let mut stmt = db.prepare(sql).map_err(|e| e.to_string())?;
        let col_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|n| (*n).to_string())
            .collect();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let row = rows
            .next()
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "query returned no rows".to_string())?;
        let out = json_row(&col_names, row)?;
        serde_json::to_vec(&json!({ "row": out })).map_err(|e| e.to_string())
    }

    fn last_insert_rowid_impl(id: u64) -> Result<i64, String> {
        let id = u32::try_from(id).map_err(|_| format!("id out of range: {id}"))?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, id)?;
        Ok(db.last_insert_rowid())
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeOpen")]
    pub fn native_open(call: &mut ExternCallContext) -> ExternResult {
        let path = call.arg_str(0);
        match open_impl(path) {
            Ok(id) => {
                call.ret_u64(0, id as u64);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_u64(0, 0);
                write_error_to(call, 1, &msg);
            }
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeOpenInMemory")]
    pub fn native_open_in_memory(call: &mut ExternCallContext) -> ExternResult {
        match open_in_memory_impl() {
            Ok(id) => {
                call.ret_u64(0, id as u64);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_u64(0, 0);
                write_error_to(call, 1, &msg);
            }
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeClose")]
    pub fn native_close(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        match close_impl(id) {
            Ok(()) => write_nil_error(call, 0),
            Err(msg) => write_error_to(call, 0, &msg),
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeExec")]
    pub fn native_exec(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        let sql = call.arg_str(1);
        match exec_impl(id, sql) {
            Ok(()) => write_nil_error(call, 0),
            Err(msg) => write_error_to(call, 0, &msg),
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeExecute")]
    pub fn native_execute(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        let sql = call.arg_str(1);
        match execute_impl(id, sql) {
            Ok(rows_affected) => {
                call.ret_u64(0, rows_affected);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_u64(0, 0);
                write_error_to(call, 1, &msg);
            }
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeQuery")]
    pub fn native_query(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        let sql = call.arg_str(1);
        match query_impl(id, sql) {
            Ok(bytes) => {
                let out_ref = call.alloc_bytes(&bytes);
                call.ret_ref(0, out_ref);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_nil(0);
                write_error_to(call, 1, &msg);
            }
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeQueryOne")]
    pub fn native_query_one(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        let sql = call.arg_str(1);
        match query_one_impl(id, sql) {
            Ok(bytes) => {
                let out_ref = call.alloc_bytes(&bytes);
                call.ret_ref(0, out_ref);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_nil(0);
                write_error_to(call, 1, &msg);
            }
        }
        ExternResult::Ok
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "nativeLastInsertRowID")]
    pub fn native_last_insert_rowid(call: &mut ExternCallContext) -> ExternResult {
        let id = call.arg_u64(0);
        match last_insert_rowid_impl(id) {
            Ok(row_id) => {
                call.ret_i64(0, row_id);
                write_nil_error(call, 1);
            }
            Err(msg) => {
                call.ret_i64(0, 0);
                write_error_to(call, 1, &msg);
            }
        }

        ExternResult::Ok
    }
}

#[cfg(feature = "native")]
vo_ext::export_extensions!();
