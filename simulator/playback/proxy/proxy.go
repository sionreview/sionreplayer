package proxy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sionreview/sion/common/util/promise"
	"github.com/sionreview/sionreplayer/simulator/readers"
	"github.com/zhangjyr/hashmap"
)

const (
	SLICE_SIZE = 100
)

var (
	FunctionOverhead uint64 = 100
	FunctionCapacity uint64 = 1536

	ErrNoPlacementsTest  = errors.New("set placements before get placements first")
	ErrPlacementsCleared = errors.New("placements cleared")
	ErrPlacementsUnset   = errors.New("placements unset")
)

type Chunk struct {
	Key   string
	Sz    uint64
	Freq  uint64
	Reset uint64
}

type Object struct {
	*readers.Record
	DChunks    int
	PChunks    int
	ChunkSz    uint64
	Estimation time.Duration // Estimate execution time
}

type Lambda struct {
	Id             uint64
	Kvs            *hashmap.HashMap // map[string]*Chunk
	MemUsed        uint64
	ActiveMinutes  int
	LastActive     int64
	Capacity       uint64
	UsedPercentile int

	block  uint64
	blocks []uint64
}

func NewLambda(id uint64) *Lambda {
	l := &Lambda{}
	l.Id = id
	l.Kvs = hashmap.New(1024)
	l.MemUsed = FunctionOverhead * 1000000  // MB
	l.Capacity = FunctionCapacity * 1000000 // MB
	return l
}

func (l *Lambda) Activate(recTime int64) {
	if l.ActiveMinutes == 0 {
		l.ActiveMinutes++
	} else if time.Duration(recTime-l.LastActive) >= time.Minute {
		l.ActiveMinutes++
	}
	l.LastActive = recTime
}

func (l *Lambda) AddChunk(chunk *Chunk, msgs ...string) {
	// msg := ""
	// if len(msgs) > 0 {
	// 	msg = msgs[0]
	// }

	l.Kvs.Set(chunk.Key, chunk)
	l.IncreaseMem(chunk.Sz)
	// used := l.IncreaseMem(chunk.Sz)
	// log.Printf("Lambda %d size tracked: %d of %d (key:%s, Δ:%d). %s", l.Id, used, l.Capacity, chunk.Key, chunk.Sz, msg)
}

func (l *Lambda) GetChunk(key string) (*Chunk, bool) {
	chunk, ok := l.Kvs.Get(key)
	if ok {
		return chunk.(*Chunk), ok
	}

	return nil, ok
}

func (l *Lambda) DelChunk(key string) (*Chunk, bool) {
	// No strict atomic is required here.
	chunk, ok := l.GetChunk(key)
	if ok {
		l.DecreaseMem(chunk.Sz)
		// used := l.DecreaseMem(chunk.Sz)
		l.Kvs.Del(key)
		// log.Printf("Lambda %d size tracked: %d of %d (key:%s, Δ:-%d).", l.Id, used, l.Capacity, chunk.Key, chunk.Sz)
		return chunk, ok
	}

	return nil, ok
}

func (l *Lambda) NumChunks() int {
	return l.Kvs.Len()
}

func (l *Lambda) AllChunks() <-chan hashmap.KeyValue {
	return l.Kvs.Iter()
}

func (l *Lambda) IncreaseMem(mem uint64) uint64 {
	return atomic.AddUint64(&l.MemUsed, mem)
}

func (l *Lambda) DecreaseMem(mem uint64) uint64 {
	return atomic.AddUint64(&l.MemUsed, ^(mem - 1))
}

type Proxy struct {
	Id           string
	LambdaPool   []*Lambda
	Balancer     ProxyBalancer
	BalancerCost time.Duration

	evicts     *hashmap.HashMap // map[string]*Chunk
	placements *hashmap.HashMap // map[string][]int
	mu         sync.Mutex
}

func NewProxy(id string, numCluster int, balancer ProxyBalancer) *Proxy {
	proxy := &Proxy{
		Id:         id,
		LambdaPool: make([]*Lambda, numCluster),
		Balancer:   balancer,
		placements: hashmap.New(1024),
		evicts:     hashmap.New(1024),
	}
	for i := 0; i < len(proxy.LambdaPool); i++ {
		proxy.LambdaPool[i] = NewLambda(uint64(i))
	}
	if balancer != nil {
		balancer.SetProxy(proxy)
		balancer.Init()
	}
	return proxy
}

func (p *Proxy) ValidateLambda(lambdaId uint64) {
	if int(lambdaId) < len(p.LambdaPool) {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	lambdaPool := p.LambdaPool
	if int(lambdaId) >= cap(p.LambdaPool) {
		lambdaPool = make([]*Lambda, cap(p.LambdaPool)*2)
		copy(lambdaPool[:len(p.LambdaPool)], p.LambdaPool)
	}
	if int(lambdaId) >= len(p.LambdaPool) {
		lambdaPool = lambdaPool[:lambdaId+1]
		for i := len(p.LambdaPool); i < len(lambdaPool); i++ {
			lambdaPool[i] = NewLambda(uint64(i))
		}
		p.LambdaPool = lambdaPool
	}
}

func (p *Proxy) Remap(placements []uint64, obj *Object) []uint64 {
	if p.Balancer == nil {
		return placements
	}

	p.Balancer.SetProxy(p)
	return p.Balancer.Remap(placements, obj)
}

func (p *Proxy) Adapt(lambdaId uint64, chk *Chunk) {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	start := time.Now()
	p.Balancer.Adapt(lambdaId, chk)
	p.BalancerCost += time.Since(start)
}

func (p *Proxy) Validate(obj *Object) bool {
	if p.Balancer == nil {
		return true
	}

	p.Balancer.SetProxy(p)
	return p.Balancer.Validate(obj)
}

func (p *Proxy) IsSet(key string) bool {
	_, ok := p.placements.Get(key)
	return ok
}

func (p *Proxy) Placements(key string) []uint64 {
	// A successful insertion can proceed, or it should wait.
	if v, ok := p.placements.GetOrInsert(key, promise.NewPromise()); !ok {
		return nil
	} else {
		if ret, err := v.(promise.Promise).Result(); err == nil {
			return ret.([]uint64)
		} else {
			// Placements cleared, retry.
			return p.Placements(key)
		}
	}
}

func (p *Proxy) SetPlacements(key string, placements []uint64) error {
	if v, ok := p.placements.Get(key); !ok {
		return ErrNoPlacementsTest
	} else {
		v.(promise.Promise).Resolve(placements)
		return nil
	}
}

func (p *Proxy) ResetPlacements(key string, placements []uint64) error {
	if v, ok := p.placements.Get(key); !ok {
		return ErrNoPlacementsTest
	} else if !v.(promise.Promise).IsResolved() {
		return ErrPlacementsUnset
	}

	ret := promise.Resolved(placements)
	p.placements.Set(key, ret)
	return nil
}

func (p *Proxy) ClearPlacements(key string) {
	v, ok := p.placements.Get(key)
	p.placements.Del(key)
	if ok && !v.(promise.Promise).IsResolved() {
		v.(promise.Promise).Resolve(nil, ErrPlacementsCleared)
	}
}

func (p *Proxy) Evict(key string, chunk *Chunk) {
	p.evicts.Set(key, chunk)
}

func (p *Proxy) GetEvicted(key string) *Chunk {
	if v, ok := p.evicts.Get(key); ok {
		return v.(*Chunk)
	} else {
		return nil
	}
}

func (p *Proxy) AllEvicts() <-chan hashmap.KeyValue {
	return p.evicts.Iter()
}

func (p *Proxy) Close() {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	p.Balancer.Close()
}
