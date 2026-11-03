package asynclog

// BackupSink receives copies of ERROR and FATAL records so they survive a
// process crash.
//
// Implementations must be non-blocking: the logger calls Backup on the hot path
// and must never be made to wait on the network. The original C++
// implementation enqueued the backup and then blocked on its future, which
// defeated the purpose of asynchronous logging; that defect is corrected here.
type BackupSink interface {
	// Backup hands a single formatted record to the sink. Records may be
	// dropped if the sink cannot keep up; implementations should count drops.
	Backup(record string)
	// Close flushes pending records and releases resources.
	Close() error
}
