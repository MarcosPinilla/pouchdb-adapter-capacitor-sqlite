// 문자열을 작은따옴표로 감싸는 함수, 모든 저장소 이름을 일관성 있게 관리하기 위해 사용
const quote = (str: string) => `'${str}'`

// 데이터베이스 마이그레이션 관리를 위한 버전 번호
const ADAPTER_VERSION = 7

// 데이터베이스 저장소 이름, 각 저장소는 고유한 목적을 가짐:
// DOC_STORE: 문서 메타데이터, 리비전 히스토리, 상태 정보를 저장
const DOC_STORE = quote('document-store')

// BY_SEQ_STORE: 특정 문서 버전을 시퀀스 ID로 저장
const BY_SEQ_STORE = quote('by-sequence')

// ATTACH_STORE: 문서에 대한 첨부 파일을 저장
const ATTACH_STORE = quote('attach-store')

// LOCAL_STORE: 다른 클라이언트와 동기화되지 않는 로컬 데이터를 저장
const LOCAL_STORE = quote('local-store')

// META_STORE: 데이터베이스의 상태와 관련된 메타데이터를 저장
const META_STORE = quote('metadata-store')

// ATTACH_AND_SEQ_STORE: 첨부 파일 다이제스트와 시퀀스 ID 간의 다대다 관계를 저장하여
// 효율적인 조회를 지원
const ATTACH_AND_SEQ_STORE = quote('attach-seq-store')

// CREATE TABLE DDL
const TABLE_QUERIES = {
  meta: `
    CREATE TABLE IF NOT EXISTS ${META_STORE} (
      dbid, 
      db_version INTEGER
    )`,

  attach: `
    CREATE TABLE IF NOT EXISTS ${ATTACH_STORE} (
      digest UNIQUE, 
      escaped TINYINT(1), 
      body BLOB
    )`,

  attachAndRev: `
    CREATE TABLE IF NOT EXISTS ${ATTACH_AND_SEQ_STORE} (
      digest, 
      seq INTEGER
    )`,

  doc: `
    CREATE TABLE IF NOT EXISTS ${DOC_STORE} (
      id UNIQUE, 
      json, 
      winningseq, 
      max_seq INTEGER UNIQUE
    )`,

  seq: `
    CREATE TABLE IF NOT EXISTS ${BY_SEQ_STORE} (
      seq INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
      json, 
      deleted TINYINT(1), 
      doc_id, 
      rev
    )`,

  local: `
    CREATE TABLE IF NOT EXISTS ${LOCAL_STORE} (
      id UNIQUE, 
      rev, 
      json
    )`,
}

// These indexes cover the ground for most allDocs queries
// INDEX DDL
const INDEX_QUERIES = {
  bySeqDeletedIndex: `
    CREATE INDEX IF NOT EXISTS 'by-seq-deleted-idx' 
    ON ${BY_SEQ_STORE} (seq, deleted)
  `,
  bySeqDocIdRevIndex: `
    CREATE UNIQUE INDEX IF NOT EXISTS 'by-seq-doc-id-rev' 
    ON ${BY_SEQ_STORE} (doc_id, rev)
  `,
  docWinningSeqIndex: `
    CREATE INDEX IF NOT EXISTS 'doc-winningseq-idx' 
    ON ${DOC_STORE} (winningseq)
  `,
  attachAndSeqSeqIndex: `
    CREATE INDEX IF NOT EXISTS 'attach-seq-seq-idx' 
    ON ${ATTACH_AND_SEQ_STORE} (seq)
  `,
  attachAndSeqDigestIndex: `
    CREATE UNIQUE INDEX IF NOT EXISTS 'attach-seq-digest-idx' 
    ON ${ATTACH_AND_SEQ_STORE} (digest, seq)
  `,
}

// 애플리케이션의 다른 부분에서 사용할 수 있도록 상수들을 내보냄
export {
  ADAPTER_VERSION,
  DOC_STORE,
  BY_SEQ_STORE,
  ATTACH_STORE,
  LOCAL_STORE,
  META_STORE,
  ATTACH_AND_SEQ_STORE,
  // 추가
  TABLE_QUERIES,
  INDEX_QUERIES,
}
