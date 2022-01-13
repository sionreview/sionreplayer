package benchclient

import (
	"context"

	"github.com/go-redis/redis/v8"
	infinicache "github.com/sionreview/sion/client"
)

var (
	AWSElasticCacheCluster = func() ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// First node with 1 master and 1 slave.
			{
				Start: 0,
				End:   3276,
				Nodes: []redis.ClusterNode{{
					Addr: "trace1.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379",
				}},
			},
			// Second node with 1 master and 1 slave.
			{
				Start: 3277,
				End:   6553,
				Nodes: []redis.ClusterNode{{
					Addr: "trace2.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379",
				}},
			},
			{
				Start: 6554,
				End:   9830,
				Nodes: []redis.ClusterNode{{
					Addr: "trace3.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
			{
				Start: 9831,
				End:   13107,
				Nodes: []redis.ClusterNode{{
					Addr: "trace4.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
			{
				Start: 13108,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: "trace5.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
		}
		return slots, nil
	}
)

type Redis struct {
	*defaultClient
	backend redis.UniversalClient
	ctx     context.Context
}

func NewRedis(addr string) *Redis {
	backend := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
	})
	return NewRedisWithBackend(backend)
}

func NewRedisWithBackend(backend redis.UniversalClient) *Redis {
	//client := newSession(addr)
	client := &Redis{
		defaultClient: newDefaultClient("Redis: "),
		backend:       backend,
		ctx:           context.Background(),
	}
	client.setter = client.set
	client.getter = client.get
	return client
}

func NewElasticCache() *Redis {
	client := &Redis{
		defaultClient: newDefaultClient("Redis: "),
	}
	client.backend = redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  client.getClusterSlots,
		RouteRandomly: true,
	})
	client.setter = client.set
	client.getter = client.get
	return client
}

func (r *Redis) set(key string, val []byte) (err error) {
	return r.backend.Set(r.ctx, key, val, 0).Err()
}

func (r *Redis) get(key string) (infinicache.ReadAllCloser, error) {
	val, err := r.backend.Get(r.ctx, key).Bytes()
	if err != nil {
		return nil, err
	} else {
		return NewByteReader(val), nil
	}
}

func (r *Redis) Close() {
	if r.backend != nil {
		r.backend.Close()
		r.backend = nil
	}
}

func (r *Redis) getClusterSlots(ctx context.Context) ([]redis.ClusterSlot, error) {
	r.ctx = ctx
	return AWSElasticCacheCluster()
}
