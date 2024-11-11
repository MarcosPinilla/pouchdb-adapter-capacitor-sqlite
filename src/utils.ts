import { createError, WSQ_ERROR, MISSING_DOC } from 'pouchdb-errors'
import {} from 'pouchdb-utils'
import {
  BY_SEQ_STORE,
  ATTACH_STORE,
  ATTACH_AND_SEQ_STORE,
  DOC_STORE,
} from './constants'
import { pick, guardedConsole } from 'pouchdb-utils'
import { latest as getLatest } from 'pouchdb-merge'
import { safeJsonParse } from 'pouchdb-json'
import { DOC_STORE_AND_BY_SEQ_JOINER, SELECT_DOCS, SQL } from './_queries'
import { SqliteService } from './_sqlite'

/**
 * 문서 객체에서 불필요한 필드(_id, _rev)를 제거하고 JSON 문자열로 변환하는 함수입니다.
 *
 * @param doc - 변환할 문서 객체
 * @returns - 변환된 JSON 문자열
 */
export function serializeDocument(doc: Record<string, any>): string {
  delete doc['_id'] // 문서에서 _id 필드 삭제
  delete doc['_rev'] // 문서에서 _rev 필드 삭제
  return JSON.stringify(doc) // 나머지 문서 객체를 JSON 문자열로 변환하여 반환
}

/**
 * JSON 문자열을 다시 문서 객체로 변환하고 _id, _rev 필드를 설정하는 함수입니다.
 *
 * @param doc - JSON 문자열로 된 문서
 * @param id - 설정할 _id 값
 * @param rev - 설정할 _rev 값
 * @returns - 변환된 문서 객체
 */
export function deserializeDocument(
  doc: string,
  id: string,
  rev: string
): Record<string, any> {
  const parsedDoc = JSON.parse(doc) // JSON 문자열을 객체로 변환
  parsedDoc._id = id // _id 필드 설정
  parsedDoc._rev = rev // _rev 필드 설정
  return parsedDoc // 변환된 문서 객체 반환
}

/**
 * SQL IN 절에서 사용될 수 있는 지정된 개수의 물음표(?)로 구성된 문자열을 생성하는 함수입니다.
 *
 * @param count - 물음표 개수
 * @returns - 물음표가 들어간 문자열 (예: "(?, ?, ?)")
 */
export function generateQuestionMarks(count: number): string {
  return `(${Array(count).fill('?').join(',')})` // 물음표를 원하는 개수만큼 연결하여 반환
}

/**
 * SELECT 쿼리 문자열을 생성하는 함수로, 선택적으로 JOIN, WHERE, ORDER BY 절을 추가할 수 있습니다.
 *
 * @param columns - 선택할 열 (예: 'id, name')
 * @param tables - 테이블 이름 (문자열 또는 문자열 배열)
 * @param joinCondition - 선택적 JOIN 조건
 * @param conditions - 선택적 WHERE 조건
 * @param orderBy - 선택적 ORDER BY 조건
 * @returns - 생성된 SQL 쿼리 문자열
 */
export function buildSelectQuery(
  columns: string,
  tables: string | string[],
  joinCondition?: string | null,
  conditions?: string | string[],
  orderBy?: string
): string {
  return (
    `SELECT ${columns} FROM ` +
    (typeof tables === 'string' ? tables : tables.join(' JOIN ')) + // 테이블 이름 처리
    (joinCondition ? ` ON ${joinCondition}` : '') + // JOIN 조건이 있을 경우 추가
    (conditions
      ? ` WHERE ${typeof conditions === 'string' ? conditions : conditions.join(' AND ')}`
      : '') + // WHERE 조건이 있을 경우 추가
    (orderBy ? ` ORDER BY ${orderBy}` : '') // ORDER BY 조건이 있을 경우 추가
  )
}

/**
 * 데이터베이스에서 오래된 리비전을 삭제하고, 사용되지 않는 첨부 파일 참조를 제거하는 함수입니다.
 *
 * @param revs - 삭제할 리비전 목록
 * @param docId - 문서 ID
 * @param tx - 트랜잭션 객체
 */
export async function removeOldRevisions(
  revs: string[],
  docId: string,
  db: SqliteService
): Promise<void> {
  if (!revs.length) return // 리비전 목록이 비어 있으면 종료

  let processedCount = 0
  const seqs: number[] = [] // 삭제할 seq를 저장할 배열

  function checkIfDone() {
    if (++processedCount === revs.length) {
      // 모든 리비전이 처리되었으면 orphaned attachments를 제거
      removeOrphanedAttachments()
    }
  }

  /**
   * 더 이상 사용되지 않는 첨부 파일 참조를 삭제하는 함수입니다.
   */
  async function removeOrphanedAttachments() {
    if (!seqs.length) return // seq가 없으면 종료

    const selectDigestSQL =
      `SELECT DISTINCT digest FROM ${ATTACH_AND_SEQ_STORE} WHERE seq IN ` +
      generateQuestionMarks(seqs.length)

    let res = await db.query<any>(selectDigestSQL, seqs)
    const digestsToCheck: string[] = []
    if (res) {
      for (let i = 0; i < res.length; i++) {
        digestsToCheck.push(res[i].digest) // orphaned attachment들의 digest를 체크
      }
    }
    if (!digestsToCheck.length) return // orphaned attachment가 없으면 종료

    // orphaned attachments의 seq 삭제
    const deleteSeqSQL = `DELETE FROM ${ATTACH_AND_SEQ_STORE} WHERE seq IN (${seqs.map(() => '?').join(',')})`
    await db.execute(deleteSeqSQL, seqs)

    const checkDigestSQL = `SELECT digest FROM ${ATTACH_AND_SEQ_STORE} WHERE digest IN (${digestsToCheck.map(() => '?').join(',')})`
    res = await db.query<any>(checkDigestSQL, digestsToCheck)

    const nonOrphanedDigests = new Set<string>()
    if (res) {
      for (let i = 0; i < res.length; i++) {
        nonOrphanedDigests.add(res[i].digest) // 여전히 사용되는 digest를 추가
      }
    }
    for (const digest of digestsToCheck) {
      if (!nonOrphanedDigests.has(digest)) {
        await db.execute(`DELETE FROM ${ATTACH_AND_SEQ_STORE} WHERE digest=?`, [
          digest, // orphaned attachment 삭제
        ])
        await db.execute(`DELETE FROM ${ATTACH_STORE} WHERE digest=?`, [digest]) // 첨부 파일 스토어에서 삭제
      }
    }
  }

  // 오래된 리비전을 삭제하고 seq 추적
  for (const rev of revs) {
    const selectSeqSQL = `SELECT seq FROM ${BY_SEQ_STORE} WHERE doc_id=? AND rev=?`
    const res = await db.query<any>(selectSeqSQL, [docId, rev])

    if (!res?.length) {
      checkIfDone() // 리비전이 없다면 처리 완료
      continue
    }

    const seq = res?.[0]?.seq
    seqs.push(seq)
    await db.execute(`DELETE FROM ${BY_SEQ_STORE} WHERE seq=?`, [seq]) // seq 삭제
  }
}

/**
 * SQLite 오류를 처리하고, 이를 로그에 기록한 후 콜백을 호출하는 함수입니다.
 *
 * @param event - 발생한 오류 객체
 * @param callback - 오류 처리 후 호출할 콜백 함수 (선택 사항)
 */
export function handleSQLiteError(
  event: Error,
  callback?: (error: any) => void
) {
  guardedConsole('error', 'SQLite threw an error', event) // 오류를 로그에 기록

  const errorName =
    event.name ||
    (event.constructor.toString().match(/function ([^(]+)/)?.[1] ??
      'UnknownError') // 오류 이름 추출
  const error = createError(WSQ_ERROR, event.message, errorName) // 오류 객체 생성
  if (callback)
    callback(error) // 콜백이 있으면 오류를 전달
  else return error // 콜백이 없으면 오류 객체 반환
}

/**
 * 필요에 따라 문서의 첨부 파일을 가져오는 함수입니다.
 * 첨부 파일이 없으면 콜백 함수를 바로 호출하고,
 * 첨부 파일이 있으면 하나씩 가져온 후 완료 여부를 확인하여 콜백을 호출합니다.
 *
 * @param doc - 첨부 파일을 포함할 수 있는 문서 객체
 * @param options - 첨부 파일 가져오기 옵션
 * @param api - 첨부 파일을 가져올 API 객체
 * @param txn - 트랜잭션 객체
 * @param cb - 모든 첨부 파일이 처리된 후 호출할 콜백 함수
 */
export function fetchAttachmentsIfNeeded(
  doc: any,
  options: any,
  api: any,
  txn: SqliteService,
  cb?: () => void
) {
  const attachments = Object.keys(doc._attachments || {})
  if (!attachments.length) {
    return cb?.()
  }
  let numProcessed = 0

  // 모든 첨부 파일이 처리되었는지 확인하는 함수
  const checkCompletion = () => {
    if (++numProcessed === attachments.length && cb) {
      cb()
    }
  }

  /**
   * 첨부 파일을 다운로드하는 함수입니다.
   * API를 통해 첨부 파일을 가져오고, 가져온 데이터를 문서에 추가합니다.
   *
   * @param doc - 첨부 파일을 포함한 문서 객체
   * @param att - 첨부 파일 이름
   */
  const downloadAttachment = (doc: any, att: string) => {
    const attachment = doc._attachments[att]
    const attachmentOptions = { binary: options.binary, ctx: txn }
    api._getAttachment(
      doc._id,
      att,
      attachment,
      attachmentOptions,
      (_: any, data: any) => {
        doc._attachments[att] = Object.assign(
          pick(attachment, ['digest', 'content_type']),
          { data }
        )
        checkCompletion()
      }
    )
  }

  // 첨부 파일을 다운로드하거나, 포함되지 않은 경우에는 stub을 추가합니다.
  attachments.forEach((att) => {
    if (options.attachments && options.include_docs) {
      downloadAttachment(doc, att)
    } else {
      doc._attachments[att].stub = true
      checkCompletion()
    }
  })
}

/**
 * 데이터베이스에서 최대 시퀀스 값을 가져오는 함수입니다.
 *
 * @param tx - 트랜잭션 객체
 * @returns - 최대 시퀀스 값
 */
export async function fetchMaxSequence(db: SqliteService): Promise<number> {
  const sql = 'SELECT MAX(seq) AS seq FROM ' + BY_SEQ_STORE
  const res = await db.query<any>(sql, [])
  return res?.[0]?.seq || 0
}

/**
 * 데이터베이스에서 총 문서 수를 세는 함수입니다.
 *
 * @param tx - 트랜잭션 객체
 * @returns - 문서 수
 */
export async function countTotalDocuments(db: SqliteService): Promise<number> {
  const sql = buildSelectQuery(
    'COUNT(' + DOC_STORE + ".id) AS 'num'",
    [DOC_STORE, BY_SEQ_STORE],
    DOC_STORE_AND_BY_SEQ_JOINER,
    BY_SEQ_STORE + '.deleted=0'
  )
  const result = await db.query<any>(sql, [])
  return result?.[0]?.num || 0
}

/**
 * 데이터베이스에서 Encoding을 얻는 함수입니다.
 *
 * @param tx - 트랜잭션 객체
 */
export async function getEncoding(db: SqliteService) {
  const res = await db.query<any>(SQL.GET_HEX)
  const hex = res?.[0]?.hex
  return hex.length === 2 ? 'UTF-8' : 'UTF-16'
}

/**
 * 주어진 문서 ID와 리비전 정보를 기반으로 최신 리비전을 가져오는 함수입니다.
 *
 * @param tx - 트랜잭션 객체
 * @param id - 문서 ID
 * @param rev - 문서 리비전
 * @param callback - 최신 리비전 정보를 받는 콜백 함수
 * @param finish - 에러 발생 시 호출되는 종료 함수
 */
export async function fetchLatestRevision(
  db: SqliteService,
  id: string,
  rev: string,
  callback: (latestRev: string) => void,
  finish: (err: any) => void
) {
  const sql = buildSelectQuery(
    SELECT_DOCS,
    [DOC_STORE, BY_SEQ_STORE],
    DOC_STORE_AND_BY_SEQ_JOINER,
    DOC_STORE + '.id=?'
  )
  const sqlArgs = [id]

  const results = await db.query<any>(sql, sqlArgs)
  if (!results?.length) {
    const err = createError(MISSING_DOC, 'missing')
    return finish(err)
  }
  const item = results?.[0]
  const metadata = safeJsonParse(item.metadata)
  callback(getLatest(rev, metadata))
}
