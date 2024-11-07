// 새로운 Transaction 타입 정의
export interface Transaction {
  executeAsync: (
    sql: string,
    params?: any[]
  ) => Promise<{
    rows: { item: (index: number) => any; length: number }
    insertId?: number
  }>
}
