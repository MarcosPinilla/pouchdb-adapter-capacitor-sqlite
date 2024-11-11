import { PluginOptions } from './_types'
import SqlPouchCore from './core'

function CapacitorSQLitePouch(
  options: PluginOptions,
  callback: (err: any) => void
) {
  try {
    // @ts-ignore
    SqlPouchCore.call(this, options, callback)
  } catch (err) {
    callback(err)
  }
}

// Set static properties
CapacitorSQLitePouch.valid = function () {
  return true
}
CapacitorSQLitePouch.use_prefix = false

export default function capacitorSqlitePlugin(PouchDB: any) {
  PouchDB.adapter('capacitor-sqlite', CapacitorSQLitePouch, true)
}
