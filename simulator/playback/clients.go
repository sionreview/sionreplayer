package main

import (
	"strings"

	"github.com/sionreview/sion/client"
	"github.com/sionreview/sionreplayer/benchclient"
)

const (
	ProviderS3      = "s3"
	ProviderRedis   = "redis"
	ProviderDummy   = "dummy"
	ProviderDefault = "default"
)

type ClientProvider func() benchclient.Client

func BuildClientProviders(options *Options) map[string]ClientProvider {
	m := make(map[string]ClientProvider)
	if options.S3 != "" {
		m[ProviderS3] = GenS3ClientProvider(options.S3)
	}
	if options.Redis != "" {
		m[ProviderRedis] = GenRedisClientProvider(options.Redis, options.RedisCluster)
	}
	if options.Dummy {
		m[ProviderDummy] = GenDummyClientProvider(options.Bandwidth, benchclient.DummyStore)
	}
	if len(m) == 0 {
		m[ProviderDefault] = GenDefaultClientProvider(options)
	}
	return m
}

func GenS3ClientProvider(bucket string) ClientProvider {
	return func() benchclient.Client {
		return benchclient.NewS3(bucket)
	}
}

func GenRedisClientProvider(addr string, cluster int) ClientProvider {
	if cluster > 1 {
		return func() benchclient.Client {
			return benchclient.NewElasticCache(addr, cluster, 0)
		}
	} else {
		return func() benchclient.Client {
			return benchclient.NewRedis(addr)
		}
	}
}

func GenDummyClientProvider(bandwidth int64, t string) ClientProvider {
	return func() benchclient.Client {
		return benchclient.NewDummy(bandwidth, t)
	}
}

func GenDefaultClientProvider(options *Options) ClientProvider {
	addrArr := strings.Split(options.AddrList, ",")
	return func() benchclient.Client {
		cli := client.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
		if !options.Dryrun {
			cli.Dial(addrArr)
		}
		return cli
	}
}
