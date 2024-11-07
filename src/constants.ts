// 문자열을 작은따옴표로 감싸는 함수
function quote(str: string): string {
  return `'${str}'`
}

// 마이그레이션 관리를 위한 어댑터 버전
const ADAPTER_VERSION = 7

// 각 데이터베이스에 대해 생성된 객체 저장소

// DOC_STORE는 문서 메타데이터, 수정 이력 및 상태를 저장
const DOC_STORE = quote('document-store')
// BY_SEQ_STORE는 시퀀스 ID를 키로 특정 버전의 문서를 저장
const BY_SEQ_STORE = quote('by-sequence')
// 첨부 파일을 저장하는 곳
const ATTACH_STORE = quote('attach-store')
// 로컬 저장소
const LOCAL_STORE = quote('local-store')
// 메타데이터 저장소
const META_STORE = quote('metadata-store')
// 첨부 파일의 다대다 관계 (다이제스트와 시퀀스 간의 관계) 저장
const ATTACH_AND_SEQ_STORE = quote('attach-seq-store')

// 필요한 상수들을 외부에서 사용할 수 있도록 내보내기
export {
  ADAPTER_VERSION,
  DOC_STORE,
  BY_SEQ_STORE,
  ATTACH_STORE,
  LOCAL_STORE,
  META_STORE,
  ATTACH_AND_SEQ_STORE,
}
