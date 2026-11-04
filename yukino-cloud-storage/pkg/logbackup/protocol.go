// Package logbackup ships ERROR and FATAL log records to a remote receiver so
// they survive a process crash.
//
// It is a Go migration of the C++ project's "backlog" component. The original
// opened a fresh TCP connection for every record, used an unframed protocol that
// silently truncated any message longer than 1024 bytes, and blocked the logging
// thread on the network round trip. This implementation uses the yukino_rpc
// framework for a framed, timeout-bounded transport, and an asynchronous bounded
// client queue so logging never blocks on the network.
package logbackup

// ServiceName is the RPC service name registered by the receiver.
const ServiceName = "LogBackup"

// AppendMethod is the RPC method that stores a batch of records.
const AppendMethod = "Append"

// AppendRequest carries a batch of formatted log records.
type AppendRequest struct {
	// Source identifies the originating process, for example a host or service
	// name. The receiver prefixes every stored record with it so batches from
	// multiple senders remain attributable.
	Source string `json:"source"`
	// Records holds one or more already-formatted log lines.
	Records []string `json:"records"`
}

// AppendReply is the receiver's acknowledgement.
type AppendReply struct {
	// Stored is the number of records the receiver persisted.
	Stored int `json:"stored"`
}
