import {
  clone,
  filterChange,
  changesHandler as Changes,
  uuid,
} from 'pouchdb-utils'
import { collectConflicts, traverseRevTree } from 'pouchdb-merge'
import { safeJsonParse, safeJsonStringify } from 'pouchdb-json'
import {
  binaryStringToBlobOrBuffer as binStringToBlob,
  btoa,
} from 'pouchdb-binary-utils'

import sqliteBulkDocs from './bulkDocs'

import { MISSING_DOC, REV_CONFLICT, createError } from 'pouchdb-errors'

import {
  DOC_STORE,
  BY_SEQ_STORE,
  ATTACH_STORE,
  LOCAL_STORE,
  META_STORE,
} from './constants'

import {
  generateQuestionMarks,
  serializeDocument,
  deserializeDocument,
  buildSelectQuery,
  removeOldRevisions,
  handleSQLiteError,
  fetchLatestRevision,
  fetchMaxSequence,
  countTotalDocuments,
  fetchAttachmentsIfNeeded,
  getEncoding,
} from './utils'
import { PluginOptions } from './_types'
import { DOC_STORE_AND_BY_SEQ_JOINER, SELECT_DOCS, SQL } from './_queries'
import { initializeDatabase } from './_migrations'
import { SqliteService } from './_sqlite'

const sqliteChanges = new Changes()

function SqlPouch(options: PluginOptions, cb: (err: any) => void) {
  // @ts-ignore
  let api = this as any
  let db: SqliteService = new SqliteService(options)

  // 초기화 함수
  async function init() {
    await db.initializeSqlite()
    await initializeDatabase(db, cb)
  }

  init()

  api.auto_compaction = false

  api._name = options.databaseName

  api._remote = false

  // DB Instance Id 반환
  api._id = (callback: (err: any, id?: string) => void) => {
    ;(async () => {
      const result = await db.query<any>(SQL.GET_DBID, [META_STORE])
      const instanceId = result?.[0]?.dbid
      callback(null, instanceId)
    })()
  }

  // DB 정보 반환
  api._info = (callback: (err: any, info?: any) => void) => {
    ;(async () => {
      try {
        callback(null, {
          doc_count: await countTotalDocuments(db),
          update_seq: await fetchMaxSequence(db),
          sqlite_encoding: (await getEncoding(db)) || 'UTF-8',
        })
      } catch (err: any) {
        handleSQLiteError(err, callback)
      }
    })()
  }

  // Bulk Docs 처리
  api._bulkDocs = async (
    req: any,
    reqOpts: any,
    callback: (err: any, response?: any) => void
  ) => {
    try {
      const response = await sqliteBulkDocs(
        { revs_limit: undefined },
        req,
        reqOpts,
        api,
        db,
        sqliteChanges
      )
      callback(null, response)
    } catch (err: any) {
      handleSQLiteError(err, callback)
    }
  }

  // Document 가져오기
  api._get = (
    id: string,
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    let doc: any
    let metadata: any
    // info : options.ctx 무시 신규 트랜잭션

    const finish = (err: any) => callback(err, { doc, metadata, ctx: db })

    const { rev, latest } = options
    let sql: string
    let sqlArgs: any[] = []

    ;(async () => {
      if (!rev) {
        sql = buildSelectQuery(
          SELECT_DOCS,
          [DOC_STORE, BY_SEQ_STORE],
          DOC_STORE_AND_BY_SEQ_JOINER,
          `${DOC_STORE}.id=?`
        )
        sqlArgs = [id]
      } else if (latest) {
        await fetchLatestRevision(
          db,
          id,
          rev,
          (latestRev: string) => {
            options.latest = false
            options.rev = latestRev
            api._get(id, options, callback)
          },
          finish
        )
        return
      } else {
        sql = buildSelectQuery(
          SELECT_DOCS,
          [DOC_STORE, BY_SEQ_STORE],
          `${DOC_STORE}.id=${BY_SEQ_STORE}.doc_id`,
          [`${BY_SEQ_STORE}.doc_id=?`, `${BY_SEQ_STORE}.rev=?`]
        )
        sqlArgs = [id, rev]
      }

      const results = await db.query<any>(sql, sqlArgs)
      if (!results.length) {
        return finish(createError(MISSING_DOC, 'missing'))
      }
      const item = results?.[0]
      metadata = safeJsonParse(item.metadata)

      if (item.deleted && !rev) {
        return finish(createError(MISSING_DOC, 'deleted'))
      }
      doc = deserializeDocument(item.data, metadata.id, item.rev)
      finish(null)
    })()
  }

  // All Docs 처리
  api._allDocs = (
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    const results: any[] = []

    const start = 'startkey' in options ? options.startkey : false
    const end = 'endkey' in options ? options.endkey : false
    const key = 'key' in options ? options.key : false
    const keys = 'keys' in options ? options.keys : false
    const descending = 'descending' in options ? options.descending : false
    let limit = 'limit' in options ? options.limit : -1
    const offset = 'skip' in options ? options.skip : 0
    const inclusiveEnd = options.inclusive_end !== false

    let sqlArgs: any[] = []
    const criteria: string[] = []
    const keyChunks: any[] = []

    if (keys) {
      const destinctKeys: string[] = []
      keys.forEach((key: string) => {
        if (destinctKeys.indexOf(key) === -1) {
          destinctKeys.push(key)
        }
      })

      for (let index = 0; index < destinctKeys.length; index += 999) {
        const chunk = destinctKeys.slice(index, index + 999)
        if (chunk.length > 0) {
          keyChunks.push(chunk)
        }
      }
    } else if (key !== false) {
      criteria.push(DOC_STORE + '.id = ?')
      sqlArgs.push(key)
    } else if (start !== false || end !== false) {
      if (start !== false) {
        criteria.push(DOC_STORE + '.id ' + (descending ? '<=' : '>=') + ' ?')
        sqlArgs.push(start)
      }
      if (end !== false) {
        let comparator = descending ? '>' : '<'
        if (inclusiveEnd) {
          comparator += '='
        }
        criteria.push(DOC_STORE + '.id ' + comparator + ' ?')
        sqlArgs.push(end)
      }
      if (key !== false) {
        criteria.push(DOC_STORE + '.id = ?')
        sqlArgs.push(key)
      }
    }

    if (!keys) {
      criteria.push(BY_SEQ_STORE + '.deleted = 0')
    }

    ;(async () => {
      const processResult = (rows: any[], results: any[], keys: any) => {
        for (let i = 0, l = rows.length; i < l; i++) {
          const item = rows[i]
          const metadata = safeJsonParse(item.metadata)
          const id = metadata.id
          const data = deserializeDocument(item.data, id, item.rev)
          const winningRev = data['_rev']
          const doc: any = {
            id: id,
            key: id,
            value: { rev: winningRev },
          }
          if (options.include_docs) {
            doc.doc = data
            doc.doc._rev = winningRev
            if (options.conflicts) {
              const conflicts = collectConflicts(metadata)
              if (conflicts.length) {
                doc.doc._conflicts = conflicts
              }
            }
            fetchAttachmentsIfNeeded(doc.doc, options, api, db)
          }
          if (item.deleted) {
            if (keys) {
              doc.value.deleted = true
              doc.doc = null
            } else {
              continue
            }
          }
          if (!keys) {
            results.push(doc)
          } else {
            let index = keys.indexOf(id)
            do {
              results[index] = doc
              index = keys.indexOf(id, index + 1)
            } while (index > -1 && index < keys.length)
          }
        }
        if (keys) {
          keys.forEach((key: string, index: number) => {
            if (!results[index]) {
              results[index] = { key: key, error: 'not_found' }
            }
          })
        }
      }

      try {
        const totalRows = await countTotalDocuments(db)
        const updateSeq = options.update_seq
          ? await fetchMaxSequence(db)
          : undefined

        if (limit === 0) {
          limit = 1
        }

        if (keys) {
          let finishedCount = 0
          const allRows: any[] = []
          for (const keyChunk of keyChunks) {
            sqlArgs = []
            criteria.length = 0
            let bindingStr = ''
            keyChunk.forEach(() => {
              bindingStr += '?,'
            })
            bindingStr = bindingStr.substring(0, bindingStr.length - 1)
            criteria.push(DOC_STORE + '.id IN (' + bindingStr + ')')
            sqlArgs = sqlArgs.concat(keyChunk)

            const sql =
              buildSelectQuery(
                SELECT_DOCS,
                [DOC_STORE, BY_SEQ_STORE],
                DOC_STORE_AND_BY_SEQ_JOINER,
                criteria,
                DOC_STORE + '.id ' + (descending ? 'DESC' : 'ASC')
              ) +
              ' LIMIT ' +
              limit +
              ' OFFSET ' +
              offset
            const result = await db.query<any>(sql, sqlArgs)
            finishedCount++
            if (result) {
              for (let index = 0; index < result.length; index++) {
                allRows.push(result?.[index])
              }
            }
            if (finishedCount === keyChunks.length) {
              processResult(allRows, results, keys)
            }
          }
        } else {
          const sql =
            buildSelectQuery(
              SELECT_DOCS,
              [DOC_STORE, BY_SEQ_STORE],
              DOC_STORE_AND_BY_SEQ_JOINER,
              criteria,
              DOC_STORE + '.id ' + (descending ? 'DESC' : 'ASC')
            ) +
            ' LIMIT ' +
            limit +
            ' OFFSET ' +
            offset
          const result = await db.query<any>(sql, sqlArgs)
          const rows: any[] = []
          if (result) {
            for (let index = 0; index < result.length; index++) {
              rows.push(result[index])
            }
          }
          processResult(rows, results, keys)
        }

        const returnVal: any = {
          total_rows: totalRows,
          offset: options.skip,
          rows: results,
        }

        if (options.update_seq) {
          returnVal.update_seq = updateSeq
        }
        callback(null, returnVal)
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
    })()
  }

  api._changes = (options: any): any => {
    options = clone(options)

    if (options.continuous) {
      const id = api._name + ':' + uuid()
      sqliteChanges.addListener(api._name, id, api, options)
      sqliteChanges.notify(api._name)
      return {
        cancel: () => {
          sqliteChanges.removeListener(api._name, id)
        },
      }
    }

    const descending = options.descending
    options.since = options.since && !descending ? options.since : 0
    let limit = 'limit' in options ? options.limit : -1
    if (limit === 0) {
      limit = 1
    }

    const results: any[] = []
    let numResults = 0

    const fetchChanges = () => {
      const selectStmt =
        DOC_STORE +
        '.json AS metadata, ' +
        DOC_STORE +
        '.max_seq AS maxSeq, ' +
        BY_SEQ_STORE +
        '.json AS winningDoc, ' +
        BY_SEQ_STORE +
        '.rev AS winningRev '
      const from = DOC_STORE + ' JOIN ' + BY_SEQ_STORE
      const joiner =
        DOC_STORE +
        '.id=' +
        BY_SEQ_STORE +
        '.doc_id' +
        ' AND ' +
        DOC_STORE +
        '.winningseq=' +
        BY_SEQ_STORE +
        '.seq'
      const criteria = ['maxSeq > ?']
      const sqlArgs = [options.since]

      if (options.doc_ids) {
        criteria.push(
          DOC_STORE + '.id IN ' + generateQuestionMarks(options.doc_ids.length)
        )
        sqlArgs.push(...options.doc_ids)
      }

      const orderBy = 'maxSeq ' + (descending ? 'DESC' : 'ASC')
      let sql = buildSelectQuery(selectStmt, from, joiner, criteria, orderBy)
      const filter = filterChange(options)

      if (!options.view && !options.filter) {
        sql += ' LIMIT ' + limit
      }

      let lastSeq = options.since || 0

      ;(async () => {
        try {
          const result = await db.query<any>(sql, sqlArgs)

          if (result) {
            for (let i = 0, l = result.length; i < l; i++) {
              const item = result[i]
              const metadata = safeJsonParse(item.metadata)
              lastSeq = item.maxSeq

              const doc = deserializeDocument(
                item.winningDoc,
                metadata.id,
                item.winningRev
              )
              const change = options.processChange(doc, metadata, options)
              change.seq = item.maxSeq

              const filtered = filter(change)
              if (typeof filtered === 'object') {
                return options.complete(filtered)
              }

              if (filtered) {
                numResults++
                if (options.return_docs) {
                  results.push(change)
                }
                if (options.attachments && options.include_docs) {
                  fetchAttachmentsIfNeeded(doc, options, api, db, () =>
                    options.onChange(change)
                  )
                } else {
                  options.onChange(change)
                }
              }
              if (numResults === limit) {
                break
              }
            }
          }

          if (!options.continuous) {
            options.complete(null, {
              results,
              last_seq: lastSeq,
            })
          }
        } catch (e: any) {
          handleSQLiteError(e, options.complete)
        }
      })()
    }

    fetchChanges()
  }

  api._close = (callback: (err?: any) => void) => {
    db.closeAllConnections()
    callback()
  }

  api._getAttachment = (
    docId: string,
    attachId: string,
    attachment: any,
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    ;(async () => {
      let res: any
      const digest = attachment.digest
      const type = attachment.content_type
      const sql =
        'SELECT escaped, body AS body FROM ' + ATTACH_STORE + ' WHERE digest=?'
      db.query<any>(sql, [digest]).then((result) => {
        const item = result?.[0]
        const data = item.body
        if (options.binary) {
          res = binStringToBlob(data, type)
        } else {
          res = btoa(data)
        }
        callback(null, res)
      })
    })()
    // info : options.ctx의 Transaction 무시, 신규 트랙잭션 사용
  }

  api._getRevisionTree = (
    docId: string,
    callback: (err: any, rev_tree?: any) => void
  ) => {
    ;(async () => {
      const sql = 'SELECT json AS metadata FROM ' + DOC_STORE + ' WHERE id = ?'
      const result = await db.query<any>(sql, [docId])
      if (!result?.length) {
        callback(createError(MISSING_DOC))
      } else {
        const data = safeJsonParse(result?.[0]?.metadata)
        callback(null, data.rev_tree)
      }
    })()
  }

  api._doCompaction = (
    docId: string,
    revs: string[],
    callback: (err?: any) => void
  ) => {
    if (!revs.length) {
      return callback()
    }
    db.executeTransaction(async () => {
      try {
        let sql = 'SELECT json AS metadata FROM ' + DOC_STORE + ' WHERE id = ?'
        const result = await db.query<any>(sql, [docId])
        const metadata = safeJsonParse(result?.[0]?.metadata)
        traverseRevTree(
          metadata.rev_tree,
          (
            isLeaf: boolean,
            pos: number,
            revHash: string,
            cdb: SqliteService,
            options: any
          ) => {
            const rev = pos + '-' + revHash
            if (revs.indexOf(rev) !== -1) {
              options.status = 'missing'
            }
          }
        )
        sql = 'UPDATE ' + DOC_STORE + ' SET json = ? WHERE id = ?'
        await db.execute(sql, [safeJsonStringify(metadata), docId])

        removeOldRevisions(revs, docId, db)
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
      callback()
    })
  }

  api._getLocal = (id: string, callback: (err: any, doc?: any) => void) => {
    ;(async () => {
      try {
        const sql = 'SELECT json, rev FROM ' + LOCAL_STORE + ' WHERE id=?'
        const res = await db.query<any>(sql, [id])
        if (res?.length) {
          const item = res?.[0]
          const doc = deserializeDocument(item.json, id, item.rev)
          callback(null, doc)
        } else {
          callback(createError(MISSING_DOC))
        }
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
    })()
  }

  api._putLocal = (
    doc: any,
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    delete doc._revisions
    const oldRev = doc._rev
    const id = doc._id
    let newRev: string
    if (!oldRev) {
      newRev = doc._rev = '0-1'
    } else {
      newRev = doc._rev = '0-' + (parseInt(oldRev.split('-')[1], 10) + 1)
    }
    const json = serializeDocument(doc)

    db.executeTransaction(async () => {
      try {
        let sql: string
        let values: any[]
        if (oldRev) {
          sql =
            'UPDATE ' + LOCAL_STORE + ' SET rev=?, json=? WHERE id=? AND rev=?'
          values = [newRev, json, id, oldRev]
        } else {
          sql = 'INSERT INTO ' + LOCAL_STORE + ' (id, rev, json) VALUES (?,?,?)'
          values = [id, newRev, json]
        }
        const res = await db.execute(sql, values)
        if (res?.changes) {
          callback(null, { ok: true, id: id, rev: newRev })
        } else {
          callback(createError(REV_CONFLICT))
        }
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
    })

    // info : options.ctx 기능 무시(신규 트랜잭션)
  }

  api._removeLocal = (
    doc: any,
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    db.executeTransaction(async () => {
      try {
        const sql = 'DELETE FROM ' + LOCAL_STORE + ' WHERE id=? AND rev=?'
        const params = [doc._id, doc._rev]
        const res = await db.execute(sql, params)
        if (!(res?.changes && res.changes > 0)) {
          return callback(createError(MISSING_DOC))
        }
        callback(null, { ok: true, id: doc._id, rev: '0-0' })
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
    })

    // info : options.ctx 기능 무시(신규 트랜잭션)
  }

  api._destroy = (
    options: any,
    callback: (err: any, response?: any) => void
  ) => {
    sqliteChanges.removeAllListeners(api._name)
    db.executeTransaction(async () => {
      try {
        for (const query of SQL.DROP_TABLES) {
          await db.execute(query, [])
        }
        callback(null, { ok: true })
      } catch (e: any) {
        handleSQLiteError(e, callback)
      }
    })
  }
}

export default SqlPouch
