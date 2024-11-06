import SQLitePouchCore from './SqlitePouchCore'
import { SqliteOptions } from './SQLiteService'

function CapacitorSQLitePouch(
  options: SqliteOptions,
  callback: (err: any) => void
) {
  try {
    // @ts-ignore
    SQLitePouchCore.call(this, options, callback)
  } catch (err) {
    callback(err)
  }
}

// Set static properties
CapacitorSQLitePouch.valid = function () {
  return true
}

CapacitorSQLitePouch.use_prefix = false

export default function CapacitorSqlitePlugin(PouchDB: any) {
  PouchDB.adapter('capacitor-sqlite', CapacitorSQLitePouch, true)
}
