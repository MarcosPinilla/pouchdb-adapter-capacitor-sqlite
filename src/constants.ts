/**
 * Wraps a string in single quotes for use in SQL queries.
 * @param str - The string to be quoted
 * @returns The quoted string
 */
function quote(str: string): string {
  return `'${str}'`
}

/**
 * Adapter version for database migrations.
 */
const ADAPTER_VERSION = 7

// Store names for database tables
const DOC_STORE = quote('document-store') // Document metadata, revision history, and state
const BY_SEQ_STORE = quote('by-sequence') // Document version, keyed by sequence ID
const ATTACH_STORE = quote('attach-store') // Stores attachments
const LOCAL_STORE = quote('local-store') // Stores local-only data
const META_STORE = quote('metadata-store') // Metadata store for the database
const ATTACH_AND_SEQ_STORE = quote('attach-seq-store') // Many-to-many relation between attachment digests and sequences

// Export constants
export {
  ADAPTER_VERSION,
  DOC_STORE,
  BY_SEQ_STORE,
  ATTACH_STORE,
  LOCAL_STORE,
  META_STORE,
  ATTACH_AND_SEQ_STORE,
}
