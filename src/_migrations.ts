import { SQL } from './_queries'
import { uuid } from 'pouchdb-utils'
import { ADAPTER_VERSION, META_STORE } from './constants'
import { SqliteService } from './_sqlite'

export async function initializeDatabase(
  db: SqliteService,
  callback: (err: any) => void
) {
  try {
    await db.executeTransaction(async () => {
      await fetchVersion(db)
    })
    callback(null)
  } catch (err) {
    callback(err)
  }
}

export async function fetchVersion(db: SqliteService) {
  const result = await db.query<any>(SQL.GET_VERSION, [META_STORE])
  if (!result?.length) {
    await onGetVersion(db, 0)
  } else if (!/db_version/.test(result?.[0]?.sql)) {
    await db.execute(SQL.ALTER_TABLE_VERSION, [META_STORE])
    await onGetVersion(db, 1)
  } else {
    const resDBVer = await db.query<any>(SQL.GET_DB_VERSION, [META_STORE])
    const dbVersion = resDBVer?.[0]?.db_version
    await onGetVersion(db, dbVersion)
  }
}

async function onGetVersion(db: SqliteService, dbVersion: number) {
  if (dbVersion === 0) {
    await createInitialSchema(db)
  } else {
    await runMigrations(db, dbVersion)
  }
}

async function createInitialSchema(db: SqliteService) {
  SQL.CREATE_TABLES.forEach((sql) => db.execute(sql))
  const instanceId = uuid()
  await db.execute(SQL.INSERT_INIT_SEQ, [
    META_STORE,
    ADAPTER_VERSION,
    instanceId,
  ])
}

async function runMigrations(db: SqliteService, dbVersion: number) {
  const migrated = dbVersion < ADAPTER_VERSION
  if (migrated) {
    await db.execute(`UPDATE ${META_STORE} SET db_version = ${ADAPTER_VERSION}`)
  }
}
