export interface DocInfo {
  _id: string
  metadata: any
  data: any
  stemmedRevs?: string[]
  error?: any
}

export interface DBOptions {
  revs_limit?: number
}

export interface Request {
  docs: any[]
}

export interface Options {
  new_edits: boolean
}
