# vo-lang/rusqlite

Vo wrapper for Rust `rusqlite` with typed query helpers.

## Module

```vo
import "github.com/vo-lang/rusqlite"
```

## Implemented API

- `Open(path)`
- `OpenInMemory()`
- `DB.Close()`
- `DB.Exec(sql)`
- `DB.Execute(sql)`
- `DB.Query(sql)`
- `DB.QueryOne(sql)`
- `DB.LastInsertRowID()`

## Build

```bash
cargo check --manifest-path rust/Cargo.toml
```
