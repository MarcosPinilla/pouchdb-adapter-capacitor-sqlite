import {
  ATTACH_STORE,
  ATTACH_AND_SEQ_STORE,
  DOC_STORE,
  BY_SEQ_STORE,
} from '../constants/constants'

// Queries for document and attachment operations
export const SELECT_COUNT_ATTACHMENT = `SELECT count(*) as cnt FROM ${ATTACH_STORE} WHERE digest=?`
export const INSERT_ATTACHMENT = `INSERT INTO ${ATTACH_STORE} (digest, body, escaped) VALUES (?,?,0)`
export const SELECT_DOC_BY_ID = `SELECT json FROM ${DOC_STORE} WHERE id = ?`
export const UPDATE_DOC_METADATA = `UPDATE ${DOC_STORE} SET json=?, max_seq=?, winningseq=(SELECT seq FROM ${BY_SEQ_STORE} WHERE doc_id=${DOC_STORE}.id AND rev=?) WHERE id=?`
export const INSERT_DOC_METADATA = `INSERT INTO ${DOC_STORE} (id, winningseq, max_seq, json) VALUES (?,?,?,?)`
export const INSERT_ATTACHMENT_MAPPING = `INSERT INTO ${ATTACH_AND_SEQ_STORE} (digest, seq) VALUES (?,?)`
export const INSERT_REVISION = `INSERT INTO ${BY_SEQ_STORE} (doc_id, rev, json, deleted) VALUES (?, ?, ?, ?);`
export const UPDATE_REVISION = `UPDATE ${BY_SEQ_STORE} SET json=?, deleted=? WHERE doc_id=? AND rev=?;`
