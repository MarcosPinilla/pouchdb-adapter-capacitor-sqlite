import { createError, WSQ_ERROR } from 'pouchdb-errors';
import { guardedConsole } from 'pouchdb-utils';
import { BY_SEQ_STORE, ATTACH_STORE, ATTACH_AND_SEQ_STORE } from '../constants/constants';

// `_id`와 `_rev` 속성을 제거하고 JSON 문자열로 변환
function stringifyDocument(doc: Record<string, any>): string {
  delete doc._id;
  delete doc._rev;
  return JSON.stringify(doc);
}

// JSON 문자열을 파싱하여 `_id`와 `_rev` 속성을 추가
function parseDocument(
  doc: string,
  id: string,
  rev: string
): Record<string, any> {
  return { ...JSON.parse(doc), _id: id, _rev: rev };
}

// IN 쿼리에서 사용할 물음표 그룹 생성 (예: 3 -> '(?,?,?)')
function generateQuestionMarks(num: number): string {
  return `(${Array(num).fill('?').join(',')})`;
}

// SQL SELECT 구문을 동적으로 생성
function buildSelectQuery(
  selector: string,
  table: string | string[],
  joiner?: string,
  where?: string | string[],
  orderBy?: string
): string {
  const tableStr = typeof table === 'string' ? table : table.join(' JOIN ');
  const whereStr = where
    ? ' WHERE ' + (typeof where === 'string' ? where : where.join(' AND '))
    : '';
  return `SELECT ${selector} FROM ${tableStr}${joiner ? ` ON ${joiner}` : ''}${whereStr}${orderBy ? ` ORDER BY ${orderBy}` : ''}`;
}

// 주어진 리비전을 삭제하고 고아로 남은 첨부 파일을 정리
async function cleanupOldRevisions(
  revs: string[],
  docId: string,
  tx: Transaction
): Promise<void> {
  if (!revs.length) return;

  const seqs: number[] = [];

  async function deleteOrphanedAttachments() {
    if (!seqs.length) return;

    let sql = `SELECT DISTINCT digest FROM ${ATTACH_AND_SEQ_STORE} WHERE seq IN ${generateQuestionMarks(seqs.length)}`;
    const digestsToCheck = (await tx.executeAsync(sql, seqs)).rows.map(row => row.digest);

    if (!digestsToCheck.length) return;

    await tx.executeAsync(`DELETE FROM ${ATTACH_AND_SEQ_STORE} WHERE seq IN (${seqs.map(() => '?').join(',')})`, seqs);
    sql = `SELECT digest FROM ${ATTACH_AND_SEQ_STORE} WHERE digest IN (${digestsToCheck.map(() => '?').join(',')})`;
    
    const nonOrphanedDigests = new Set((await tx.executeAsync(sql, digestsToCheck)).rows.map(row => row.digest));

    for (const digest of digestsToCheck) {
      if (!nonOrphanedDigests.has(digest)) {
        await tx.executeAsync(`DELETE FROM ${ATTACH_AND_SEQ_STORE} WHERE digest=?`, [digest]);
        await tx.executeAsync(`DELETE FROM ${ATTACH_STORE} WHERE digest=?`, [digest]);
      }
    }
  }

  // BY_SEQ_STORE에서 리비전 삭제 및 seq 저장
  for (const rev of revs) {
    const res = await tx.executeAsync(`SELECT seq FROM ${BY_SEQ_STORE} WHERE doc_id=? AND rev=?`, [docId, rev]);
    if (!res.rows?.length) return;
    
    const seq = res.rows.item(0).seq;
    seqs.push(seq);

    await tx.executeAsync(`DELETE FROM ${BY_SEQ_STORE} WHERE seq=?`, [seq]);
  }

  await deleteOrphanedAttachments();
}

// SQLite 오류를 처리하여 콘솔에 로그 및 오류 객체 생성
export function handleDatabaseError(
  event: Error,
  callback?: (error: any) => void
) {
  guardedConsole('error', 'SQLite threw an error', event);
  
  const errorNameMatch = event?.constructor.toString().match(/function ([^(]+)/);
  const errorName = errorNameMatch?.[1] || event.name;
  const error = createError(WSQ_ERROR, event.message, errorName);

  if (callback) callback(error);
  else return error;
}

export { stringifyDocument, parseDocument, generateQuestionMarks, buildSelectQuery, cleanupOldRevisions };
