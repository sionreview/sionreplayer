package benchclient

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	sion "github.com/sionreview/sion/client"
	"github.com/zhangjyr/hashmap"
)

const (
	DummyStore          = "ds"
	DummyCache          = "dc"
	DummyCacheMissRatio = 50 // Miss ratio of cache, 10 means 10%
)

var (
	sizemap *hashmap.HashMap
)

func ResetDummySizeRegistry() {
	sizemap = hashmap.New(10000)
}

type Dummy struct {
	*defaultClient
	ctx       context.Context
	bandwidth int64
}

// NewDummy returns a new dummy client.
// bandwidth defined the bandwidth of the dummy client in B/s, 0 for unlimited.
func NewDummy(bandwidth int64, t string) *Dummy {
	//client := newSession(addr)
	client := &Dummy{
		defaultClient: newDefaultClient(fmt.Sprintf("Dummy%s: ", strings.ToUpper(t))),
		ctx:           context.Background(),
		bandwidth:     bandwidth,
	}
	client.setter = client.set
	client.getter = client.get
	client.abbr = t
	return client
}

func (d *Dummy) set(key string, val []byte) (err error) {
	sizemap.Set(key, len(val))

	if d.bandwidth == 0 {
		return nil
	}

	time.Sleep(d.sizeToDuration(len(val)))
	return nil
}

func (d *Dummy) get(key string) (sion.ReadAllCloser, error) {
	size, ok := sizemap.Get(key)
	if !ok {
		return nil, sion.ErrNotFound
	}

	if d.abbr == DummyCache && rand.Intn(100) < DummyCacheMissRatio {
		return nil, sion.ErrNotFound
	}

	if d.bandwidth == 0 {
		return &DummyReadAllCloser{size: size.(int)}, nil
	}
	time.Sleep(d.sizeToDuration(size.(int)))
	return &DummyReadAllCloser{size: size.(int)}, nil
}

func (d *Dummy) sizeToDuration(size int) time.Duration {
	return time.Duration(float64(size) / float64(d.bandwidth) * float64(time.Second))
}

type DummyReadAllCloser struct {
	size int
}

func (r *DummyReadAllCloser) Len() int {
	return r.size
}

func (r *DummyReadAllCloser) Read(p []byte) (n int, err error) {
	return n, ErrNotSupported
}

func (r *DummyReadAllCloser) ReadAll() ([]byte, error) {
	return nil, ErrNotSupported
}

func (r *DummyReadAllCloser) Close() error {
	return nil
}
