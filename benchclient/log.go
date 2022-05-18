package benchclient

import (
	"fmt"
	"os"
	"time"

	"github.com/ScottMansfield/nanolog"
)

var (
	logClient nanolog.Handle
	nlogger   func(nanolog.Handle, ...interface{}) error
)

func init() {
	// cmd, reqId, begin, duration, size, ret, client
	logClient = nanolog.AddLogger("%s,%s,%i64,%i64,%i,%i,%s")
}

type logEntry struct {
	Cmd      string
	ReqId    string
	Start    time.Time
	Duration time.Duration
	Size     int
	Ret      int
}

func (e *logEntry) Begin(reqId string) {
	e.ReqId = reqId
	e.Start = time.Now()
}

func (e *logEntry) Since() time.Duration {
	return time.Since(e.Start)
}

// CreateLog Enabling evaluation log in client lib.
func CreateLog(opts map[string]interface{}) {
	path := opts["file"].(string) + "_bench.clog"
	nanoLogout, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		panic(err)
	}
	SetLogger(nanolog.Log)
}

// FlushLog Flush logs to the file.y
func FlushLog() {
	if err := nanolog.Flush(); err != nil {
		fmt.Println("log flush err")
	}
}

// SetLogger set customized evaluation logger
func SetLogger(l func(nanolog.Handle, ...interface{}) error) {
	nlogger = l
}

func nanoLog(handle nanolog.Handle, args ...interface{}) error {
	if nlogger == nil {
		return nil
	}

	if len(args) > 0 {
		entry, ok := args[0].(*logEntry)
		if ok {
			return nlogger(handle, entry.Cmd, entry.ReqId, entry.Start.UnixNano(), entry.Duration.Nanoseconds(), entry.Size, entry.Ret)
		}
	}
	return nlogger(handle, args...)
}
