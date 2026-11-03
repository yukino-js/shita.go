package asynclog

import (
	"strconv"
	"time"
)

// Message is a single log record together with the metadata needed to render
// it.
type Message struct {
	Level       Level
	LoggerName  string
	File        string
	Line        int
	Payload     string
	Time        time.Time
	GoroutineID int64
}

// Format renders the record in the canonical layout:
//
//	[2006-01-02 15:04:05.000][g:123][INFO][logger][file.go:42]\tpayload\n
//
// The timestamp carries a date component and millisecond precision so records
// produced on different days, or within the same second, remain unambiguous;
// the original C++ layout printed only HH:MM:SS.
func (m *Message) Format() string {
	buf := make([]byte, 0, 96+len(m.Payload))
	buf = append(buf, '[')
	buf = m.Time.AppendFormat(buf, "2006-01-02 15:04:05.000")
	buf = append(buf, ']', '[', 'g', ':')
	buf = strconv.AppendInt(buf, m.GoroutineID, 10)
	buf = append(buf, ']', '[')
	buf = append(buf, m.Level.String()...)
	buf = append(buf, ']', '[')
	buf = append(buf, m.LoggerName...)
	buf = append(buf, ']', '[')
	buf = append(buf, m.File...)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, int64(m.Line), 10)
	buf = append(buf, ']', '\t')
	buf = append(buf, m.Payload...)
	buf = append(buf, '\n')
	return string(buf)
}
