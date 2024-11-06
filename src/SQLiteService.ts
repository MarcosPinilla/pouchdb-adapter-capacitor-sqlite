import {
  CapacitorSQLite,
  SQLiteConnection,
  SQLiteDBConnection,
  CapacitorSQLitePlugin,
  capSQLiteUpgradeOptions,
  capSQLiteResult,
  DBSQLiteValues,
  capSQLiteChanges,
  JsonSQLite,
} from "@capacitor-community/sqlite";
import { defineCustomElements as jeepSqlite } from "jeep-sqlite/loader";

export type DbOptions = {name: string, version: number, readonly: boolean, platform: string};

export type QueryResult = {
  changes?: number;
  lastId?: number;
};


export class SQLiteService {
  sqlitePlugin!: CapacitorSQLitePlugin;
  sqliteConnection!: SQLiteConnection;
  platform!: string;
  databaseName!: string;
  native: boolean = false;
  isService: boolean = false;
  // From DatabaseService
  db!: SQLiteDBConnection;
  databaseVersion!: number;

  constructor(platform: string, databaseName: string, databaseVersion: number) {
    this.platform = platform;
    this.databaseName = databaseName;
    this.databaseVersion = databaseVersion;
  }

  async initializeSqlite() {
    try {
      await this.initializePlugin();

      if (this.getPlatform() === "web") {
        await this.initWebStore();
      }
      // initializeDatabase();
      // createInitialData();
    } catch (error) {
      console.error("initializeSqlite : ", error);
    }
    console.log(`initializeSqlite`);
  }

  /**
   * Plugin Initialization
   */
  async initializePlugin(): Promise<boolean> {
    if (this.platform === "ios" || this.platform === "android") {
      this.native = true;
    }
    this.sqlitePlugin = CapacitorSQLite;
    this.sqliteConnection = new SQLiteConnection(this.sqlitePlugin);
    this.isService = true;
    return true;
  }

  /**
   * Get Platform
   */
  getPlatform() {
    return this.platform;
  }

  /**
   * Initialize the Web store
   */
  async initWebStore(): Promise<void> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      // jeepSqlite 설정 포함
      jeepSqlite(window);
      const jeepEl = document.createElement("jeep-sqlite");
      document.body.appendChild(jeepEl);
      await customElements.whenDefined('jeep-sqlite');
      jeepEl.autoSave = false;

      await this.sqliteConnection.initWebStore();

      await jeepEl.isStoreOpen();
    } catch (err) {
      throw Error(`initWebStore: ${err}`);
    }
  }

  /**
   * Open Database Connection
   * - Create Connection or Use Existed Connection.
   */
  async openDbConnection(
    dbName: string,
    encrypted: boolean,
    encryptedMode: string = "no-encryption",
    version: number,
    readonly: boolean
  ): Promise<SQLiteDBConnection> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    let db: SQLiteDBConnection;
    const isConsistency = (await this.sqliteConnection.checkConnectionsConsistency()).result;
    const isConnection = (await this.sqliteConnection.isConnection(dbName, readonly)).result;
    if (isConsistency && isConnection) {
      db = await this.sqliteConnection.retrieveConnection(dbName, readonly);
    } else {
      db = await this.sqliteConnection.createConnection(dbName, encrypted, encryptedMode, version, readonly);
      await db.open();
    }
    return db;
  }

  /**
   * Open Database Connection
   * - Create Connection or Use Existed Connection.
   */
  async getDbConnection(dbName: string, loadToVersion: number): Promise<SQLiteDBConnection> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    return await this.openDbConnection(dbName, false, "no-encryption", loadToVersion, false);
  }

  /**
   * Create Upgrade Statements
   */
  async addUpgradeStatement(options: capSQLiteUpgradeOptions): Promise<void> {
    if (this.sqlitePlugin === null) {
      throw Error(`no plugin initialized`);
    }
    await this.sqlitePlugin.addUpgradeStatement(options);
    return;
  }

  /**
   * Save To Web Store
   */
  async saveToWebStore(dbName: string): Promise<void> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    await this.sqliteConnection.saveToStore(dbName);
  }

  /**
   * Is Database Encrypted
   */
  async isDatabaseEncrypted(dbName: string): Promise<capSQLiteResult> {
    const isDB = (await this.sqliteConnection.isDatabase(dbName)).result;
    if (!isDB) {
      return { result: false };
    }
    return await this.sqliteConnection.isDatabaseEncrypted(dbName);
  }

  /**
   * Execute Select Query
   */
  async executeSelectQuery(statement: string, values: any[] = [], db: SQLiteDBConnection): Promise<DBSQLiteValues> {
    try {
      return await db.query(statement, values);
    } catch (err) {
      throw Error(`executeQuery: ${err}`);
    }
  }

  /**
   * Execute NonSelect Query
   */
  async executeNonSelectQuery(
    statement: string,
    values: any[] = [],
    db: SQLiteDBConnection,
    autoCommit = false
  ): Promise<capSQLiteChanges> {
    const ret: capSQLiteChanges = await db.run(statement, values, autoCommit);
    if (!ret?.changes?.changes || ret.changes.changes < 0) {
      throw Error(`execute Query fail: ${statement}`);
    }
    return ret;
  }

  /**
   * ExecuteTransaction
   */
  async executeTransaction(
    transaction: (db: SQLiteDBConnection) => Promise<void>,
    db: SQLiteDBConnection,
    databaseName: string
  ): Promise<void> {
    try {
      await db.execute("BEGIN TRANSACTION;", false);
      await transaction(db);
      await db.execute("COMMIT;", false);
      if (this.getPlatform() === "web") {
        await this.saveToWebStore(databaseName);
      }
    } catch (err) {
      console.error(err);
      await db.execute("ROLLBACK;", false);
    }
  }

  /**
   * Execute Simple Transaction
   */
  async executeSimpleTransaction(
    queries: { statement: string; values?: any[] }[],
    db: SQLiteDBConnection,
    databaseName: string
  ): Promise<void> {
    try {
      await db.executeTransaction(queries);
      if (this.getPlatform() === "web") {
        await this.saveToWebStore(databaseName);
      }
    } catch (err) {
      throw Error(`executeQuery: ${err}`);
    }
  }

  /**
   * export to local disk for backup(Popup, Only Web)
   */
  async saveToLocalDisk(databaseName: string) {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.saveToLocalDisk(databaseName);
    } catch (err) {
      throw Error(`saveToLocalDisk: ${err}`);
    }
  }

  /**
   * import to app for restore(Popup, Only Web)
   */
  async getFromLocalDiskToStore(overwrite = true) {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.getFromLocalDiskToStore();
    } catch (err) {
      throw Error(`getFromLocalDiskToStore: ${err}`);
    }
  }

  /**
   * close all connections
   */
  async closeAllConnections() {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.closeAllConnections();
    } catch (err) {
      throw Error(`closeAllConnections: ${err}`);
    }
  }

  /**
   * getMigratableDbList
   */
  async getMigratableDbList() {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      return (await this.sqliteConnection.getMigratableDbList("default")).values || [];
    } catch (err) {
      throw Error(`getMigratableDbList: ${err}`);
    }
  }

  /**
   * Is Json Object Valid
   */
  async isJsonValid(jsonstring: string): Promise<boolean> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      return (await this.sqliteConnection.isJsonValid(jsonstring))?.result || false;
    } catch (err) {
      throw Error(`isJsonValid: ${err}`);
    }
  }

  /**
   * Import from Json Object
   */
  async importFromJson(jsonstring: string) {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.importFromJson(jsonstring);
    } catch (err) {
      throw Error(`importFromJson: ${err}`);
    }
  }

  /**
   * Export to Json Object
   */
  async exportToJson(databaseName: string): Promise<JsonSQLite | undefined> {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      return (await this.sqlitePlugin.exportToJson({ database: databaseName, jsonexportmode: "full" })).export;
    } catch (err) {
      throw Error(`exportToJson: ${err}`);
    }
  }

  /**
   * moveDatabasesAndAddSuffix
   */
  async moveDatabasesAndAddSuffix() {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.moveDatabasesAndAddSuffix("test");
    } catch (err) {
      throw Error(`importFromJson: ${err}`);
    }
  }

  /**
   * copyFromAssets
   */
  async copyFromAssets() {
    if (this.sqliteConnection === null) {
      throw Error(`no plugin connection`);
    }
    try {
      await this.sqliteConnection.copyFromAssets();
    } catch (err) {
      throw Error(`exportToJson: ${err}`);
    }
  }


  // Database Service
  public async getDbConnection() {
    return await this.sqliteService.getDbConnection(this.databaseName, this.databaseVersion);
  }

  async query<T>(statement: string, values: any[] = []): Promise<T[]> {
    console.log(`select query : ${statement} / ${values}`);
    this.db = await this.getDbConnection();
    const response = await this.sqliteService.executeSelectQuery(statement, values, this.db);

    // Json String을 Json 객체로 변환
    const result: T[] = response.values as T[];
    result.forEach((row: T) => {
      const jsonColumnList = ["statusList"];
      jsonColumnList.forEach((column) => {
        if (row?.[column]) {
          try {
            row[column] = JSON.parse(row[column]);
          } catch (e) {
            console.error("query json parsing error : ", column, " : ", e);
          }
        }
      });
    });

    console.log(`select result : ${JSON.stringify(result)}`);
    return result;
  }

  async executeTransaction(transaction: (db?: SQLiteDBConnection) => Promise<void>): Promise<void> {
    this.db = await this.getDbConnection();
    await this.executeTransaction(transaction, this.db, this.databaseName);
  }

  async execute(statement: string, values: any[] = []): Promise<QueryResult | undefined> {
    if (this.db === null) {
      throw Error(`executeTransaction getDbConnection fail`);
    }
    console.log(`execute query : ${statement} / ${values}`);
    const response = await this.executeNonSelectQuery(statement, values, this.db, false);
    console.log(`execute result : ${JSON.stringify(response)}`);
    return response.changes;
  }

  async executeSimpleTransaction(queries: { statement: string; values?: any[] }[]): Promise<void> {
    this.db = await this.getDbConnection();
    await this.executeSimpleTransaction(queries, this.db, this.databaseName);
  }
}
