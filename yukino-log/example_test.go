package yukino_log_test

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	yukino_log "github.com/yukino-js/yukino-log"
)

func ExampleLogger() {
	var buf bytes.Buffer
	logger := yukino_log.New(yukino_log.Options{
		Output: buf.Write,
		Now: func() time.Time {
			return time.Date(2024, 3, 1, 8, 30, 0, 123456000, time.UTC)
		},
		Location: time.UTC,
	})
	logger.Infof("hello %s", "world")
	head, _, _ := strings.Cut(buf.String(), " - ")
	fmt.Println(head)
	// Output:
	// 2024/03/01 08:30:00.123456 INFO  hello world
}
