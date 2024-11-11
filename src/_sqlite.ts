import {
  CapacitorSQLite,
  SQLiteConnection,
  SQLiteDBConnection,
  CapacitorSQLitePlugin,
  DBSQLiteValues,
  capSQLiteChanges,
} from '@capacitor-community/sqlite'
import { defineCustomElements as jeepSqlite } from 'jeep-sqlite/loader'

export type SqliteOptions = {
  databaseName: string
  databaseVersion: number
  platform: string
}

export type QueryResult = {
  changes?: number
  lastId?: number
}

export class SqliteService {
  sqlitePlugin!: CapacitorSQLitePlugin
  sqliteConnection!: SQLiteConnection
  dbConnection!: SQLiteDBConnection

  platform!: string
  databaseName!: string
  databaseVersion!: number
  readonly: boolean = false

  native: boolean = false
  isService: boolean = false

  constructor(sqliteOptions: SqliteOptions) {
    const { platform, databaseName, databaseVersion } = sqliteOptions
    this.platform = platform
    this.databaseName = databaseName
    this.databaseVersion = databaseVersion
  }

  /**
   * Sqlite Initialization
   */
  public async initializeSqlite() {
    try {
      await this.initializePlugin()

      if (this.getPlatform() === 'web') {
        await this.initWebStore()
      }
    } catch (error) {
      console.error('initializeSqlite : ', error)
    }
    console.log(`initializeSqlite`)
  }

  /**
   * Plugin Initialization
   */
  private async initializePlugin(): Promise<boolean> {
    if (this.platform === 'ios' || this.platform === 'android') {
      this.native = true
    }
    this.sqlitePlugin = CapacitorSQLite
    this.sqliteConnection = new SQLiteConnection(this.sqlitePlugin)
    this.isService = true
    return true
  }

  /**
   * Get Platform
   */
  private getPlatform() {
    return this.platform
  }

  /**
   * Initialize the Web store
   */
  private async initWebStore(): Promise<void> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`)
    }
    try {
      // jeepSqlite 설정 포함
      jeepSqlite(window)
      const jeepEl = document.createElement('jeep-sqlite')
      document.body.appendChild(jeepEl)
      await customElements.whenDefined('jeep-sqlite')
      jeepEl.autoSave = false

      await this.sqliteConnection.initWebStore()

      await jeepEl.isStoreOpen()
    } catch (err) {
      throw Error(`initWebStore: ${err}`)
    }
  }

  /**
   * Open Database Connection
   * - Create Connection or Use Existed Connection.
   */
  private async openDbConnection(
    databaseName: string,
    encrypted: boolean,
    encryptedMode: string = 'no-encryption',
    version: number,
    readonly: boolean
  ): Promise<SQLiteDBConnection> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`)
    }
    let db: SQLiteDBConnection
    const isConsistency = (
      await this.sqliteConnection.checkConnectionsConsistency()
    ).result
    const isConnection = (
      await this.sqliteConnection.isConnection(databaseName, readonly)
    ).result
    if (isConsistency && isConnection) {
      db = await this.sqliteConnection.retrieveConnection(
        databaseName,
        readonly
      )
    } else {
      db = await this.sqliteConnection.createConnection(
        databaseName,
        encrypted,
        encryptedMode,
        version,
        readonly
      )
      await db.open()
    }
    return db
  }

  /**
   * Open Database Connection
   * - Create Connection or Use Existed Connection.
   */
  private async getSqliteDbConnection(
    databaseName: string,
    loadToVersion: number
  ): Promise<SQLiteDBConnection> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`)
    }
    return await this.openDbConnection(
      databaseName,
      false,
      'no-encryption',
      loadToVersion,
      this.readonly
    )
  }

  /**
   * Save To Web Store
   */
  private async saveToWebStore(databaseName: string): Promise<void> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`)
    }
    await this.sqliteConnection.saveToStore(databaseName)
  }

  /**
   * Execute Select Query
   */
  private async executeSelectQuery(
    statement: string,
    values: any[] = [],
    db: SQLiteDBConnection
  ): Promise<DBSQLiteValues> {
    try {
      return await db.query(statement, values)
    } catch (err) {
      throw Error(`executeQuery: ${err}`)
    }
  }

  /**
   * Execute NonSelect Query
   */
  private async executeNonSelectQuery(
    statement: string,
    values: any[] = [],
    db: SQLiteDBConnection,
    autoCommit = false
  ): Promise<capSQLiteChanges> {
    const ret: capSQLiteChanges = await db.run(statement, values, autoCommit)
    if (!ret?.changes?.changes || ret.changes.changes < 0) {
      throw Error(`execute Query fail: ${statement}`)
    }
    return ret
  }

  /**
   * ExecuteTransaction
   */
  private async executeSqliteTransaction(
    transaction: (db: SQLiteDBConnection) => Promise<void>,
    db: SQLiteDBConnection,
    databaseName: string
  ): Promise<void> {
    try {
      await db.execute('BEGIN TRANSACTION;', false)
      await transaction(db)
      await db.execute('COMMIT;', false)
      if (this.getPlatform() === 'web') {
        await this.saveToWebStore(databaseName)
      }
    } catch (err) {
      console.error(err)
      await db.execute('ROLLBACK;', false)
    }
  }

  /**
   * Execute Simple Transaction
   */
  private async executeSimpleSqliteTransaction(
    queries: { statement: string; values?: any[] }[],
    db: SQLiteDBConnection,
    databaseName: string
  ): Promise<void> {
    try {
      await db.executeTransaction(queries)
      if (this.getPlatform() === 'web') {
        await this.saveToWebStore(databaseName)
      }
    } catch (err) {
      throw Error(`executeQuery: ${err}`)
    }
  }

  /**
   * close all connections
   */
  public async closeAllConnections() {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`)
    }
    try {
      await this.sqliteConnection.closeAllConnections()
    } catch (err) {
      throw Error(`closeAllConnections: ${err}`)
    }
  }

  // Database Service
  private async getDbConnection() {
    return await this.getSqliteDbConnection(
      this.databaseName,
      this.databaseVersion
    )
  }

  public async query<T>(statement: string, values: any[] = []): Promise<T[]> {
    console.log(`buildSelectQuery query : ${statement} / ${values}`)
    this.dbConnection = await this.getDbConnection()
    const response = await this.executeSelectQuery(
      statement,
      values,
      this.dbConnection
    )

    // Json String을 Json 객체로 변환
    const result: T[] = response.values as T[]
    result.forEach((row: T) => {
      const jsonColumnList = ['statusList']
      jsonColumnList.forEach((column) => {
        const rowObj: any = row
        if (rowObj?.[column]) {
          try {
            rowObj[column] = JSON.parse(rowObj[column])
          } catch (e) {
            console.error('query json parsing error : ', column, ' : ', e)
          }
        }
      })
    })

    console.log(`buildSelectQuery result : ${JSON.stringify(result)}`)
    return result
  }

  public async executeTransaction(
    transaction: (dbConnection?: SQLiteDBConnection) => Promise<void>
  ): Promise<void> {
    this.dbConnection = await this.getDbConnection()
    await this.executeSqliteTransaction(
      transaction,
      this.dbConnection,
      this.databaseName
    )
  }

  public async execute(
    statement: string,
    values: any[] = []
  ): Promise<QueryResult | undefined> {
    if (this.dbConnection === null) {
      throw Error(`execute getDbConnection fail`)
    }
    console.log(`execute query : ${statement} / ${values}`)
    const response = await this.executeNonSelectQuery(
      statement,
      values,
      this.dbConnection,
      false
    )
    console.log(`execute result : ${JSON.stringify(response)}`)
    return response.changes
  }

  async executeSimpleTransaction(
    queries: { statement: string; values?: any[] }[]
  ): Promise<void> {
    this.dbConnection = await this.getDbConnection()
    await this.executeSimpleSqliteTransaction(
      queries,
      this.dbConnection,
      this.databaseName
    )
  }
}
