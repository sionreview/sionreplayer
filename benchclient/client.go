package benchclient

import (
	"errors"
	"time"

	"github.com/google/uuid"
	sion "github.com/sionreview/sion/client"
	"github.com/sionreview/sion/common/logger"
)

const (
	ResultSuccess  = 0
	ResultError    = 1
	ResultNotFound = 2
)

func resultFromError(err error) int {
	switch err {
	case nil:
		return ResultSuccess
	case sion.ErrNotFound:
		return ResultNotFound
	default:
		return ResultError
	}
}

var (
	ErrNotSupported = errors.New("not supported")
)

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, error)
	EcGet(string, ...interface{}) (string, sion.ReadAllCloser, error)
	Close()
}

type clientSetter func(string, []byte) error
type clientGetter func(string) (sion.ReadAllCloser, error)

type defaultClient struct {
	log    logger.ILogger
	setter clientSetter
	getter clientGetter
	abbr   string // Abbreviation for logging
}

func newDefaultClient(logPrefix string) *defaultClient {
	return newDefaultClientWithAccessor(logPrefix, nil, nil)
}

func newDefaultClientWithAccessor(logPrefix string, setter clientSetter, getter clientGetter) *defaultClient {
	return &defaultClient{
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  logPrefix,
		},
		setter: setter,
		getter: getter,
		abbr:   "na",
	}
}

func (c *defaultClient) EcSet(key string, val []byte, args ...interface{}) (string, error) {
	reqId := uuid.New().String()

	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil
	}

	if c.setter == nil {
		return reqId, ErrNotSupported
	}

	// Timing
	start := time.Now()
	err := c.setter(key, val)
	duration := time.Since(start)
	nanoLog(logClient, "set", key, start.UnixNano(), duration.Nanoseconds(), len(val), resultFromError(err), c.abbr)
	if err != nil {
		c.log.Error("Failed to upload: %v", err)
		return reqId, err
	}
	c.log.Info("Set %s %v %d", key, duration, len(val))
	return reqId, nil
}

func (c *defaultClient) EcGet(key string, args ...interface{}) (string, sion.ReadAllCloser, error) {
	reqId := uuid.New().String()

	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, nil
	}

	if c.getter == nil {
		return reqId, nil, ErrNotSupported
	}

	// Timing
	start := time.Now()
	reader, err := c.getter(key)
	duration := time.Since(start)
	size := 0
	if reader != nil {
		size = reader.Len()
	}
	nanoLog(logClient, "get", key, start.UnixNano(), duration.Nanoseconds(), size, resultFromError(err), c.abbr)
	if err != nil {
		c.log.Error("failed to download: %v", err)
		return reqId, nil, err
	}
	c.log.Info("Get %s %v %d", key, duration, size)
	return reqId, reader, nil
}

func (c *defaultClient) Close() {
	// Nothing
}
