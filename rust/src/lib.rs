use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use serde::Deserialize;
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

    #[derive(Deserialize)]
    struct OpenReq {
        path: String,
    }

    #[derive(Deserialize)]
    struct IdReq {
        id: u32,
    }

    #[derive(Deserialize)]
    struct SqlReq {
        id: u32,
        sql: String,
    }

    fn empty_ok() -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }

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

    fn handle_open(input: &str) -> Result<Vec<u8>, String> {
        let req: OpenReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let conn = Connection::open(&req.path).map_err(|e| e.to_string())?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.insert(id, conn);
        serde_json::to_vec(&json!({ "id": id })).map_err(|e| e.to_string())
    }

    fn handle_open_memory(_input: &str) -> Result<Vec<u8>, String> {
        let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.insert(id, conn);
        serde_json::to_vec(&json!({ "id": id })).map_err(|e| e.to_string())
    }

    fn handle_close(input: &str) -> Result<Vec<u8>, String> {
        let req: IdReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        dbs.remove(&req.id)
            .ok_or_else(|| format!("invalid db id {}", req.id))?;
        empty_ok()
    }

    fn handle_exec(input: &str) -> Result<Vec<u8>, String> {
        let req: SqlReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db_mut(&mut dbs, req.id)?;
        db.execute_batch(&req.sql).map_err(|e| e.to_string())?;
        empty_ok()
    }

    fn handle_execute(input: &str) -> Result<Vec<u8>, String> {
        let req: SqlReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let mut dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db_mut(&mut dbs, req.id)?;
        let rows_affected = db.execute(&req.sql, []).map_err(|e| e.to_string())?;
        serde_json::to_vec(&json!({ "rows_affected": rows_affected as u64 }))
            .map_err(|e| e.to_string())
    }

    fn handle_query(input: &str) -> Result<Vec<u8>, String> {
        let req: SqlReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, req.id)?;

        let mut stmt = db.prepare(&req.sql).map_err(|e| e.to_string())?;
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

    fn handle_query_one(input: &str) -> Result<Vec<u8>, String> {
        let req: SqlReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, req.id)?;

        let mut stmt = db.prepare(&req.sql).map_err(|e| e.to_string())?;
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

    fn handle_last_insert_rowid(input: &str) -> Result<Vec<u8>, String> {
        let req: IdReq = serde_json::from_str(input).map_err(|e| e.to_string())?;
        let dbs = DBS
            .lock()
            .map_err(|_| "rusqlite lock poisoned".to_string())?;
        let db = get_db(&dbs, req.id)?;
        serde_json::to_vec(&json!({ "id": db.last_insert_rowid() })).map_err(|e| e.to_string())
    }

    fn dispatch(op: &str, input: &str) -> Result<Vec<u8>, String> {
        match op {
            "open" => handle_open(input),
            "open_memory" => handle_open_memory(input),
            "close" => handle_close(input),
            "exec" => handle_exec(input),
            "execute" => handle_execute(input),
            "query" => handle_query(input),
            "query_one" => handle_query_one(input),
            "last_insert_rowid" => handle_last_insert_rowid(input),
            _ => Err(format!("unsupported operation: {op}")),
        }
    }

    #[vo_fn("github.com/vo-lang/rusqlite", "RawCall")]
    pub fn raw_call(call: &mut ExternCallContext) -> ExternResult {
        let op = call.arg_str(0);
        let input = call.arg_str(1);

        match dispatch(op, input) {
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
}

#[cfg(feature = "native")]
vo_ext::export_extensions!();
