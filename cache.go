package compressedcache

import (
	"sync"
	"time"

	"github.com/NikoMalik/compressed_cache/compress"
	minder "github.com/NikoMalik/minder"
)

type Cache[K comparable, V any] interface {

	// Get get cache data by key
	Get(k K) (V, bool)

	// Set set new cache data
	Set(k K, v V) bool

	// Remove remove the specified key
	Del(k K)

	SetWithTTL(k K, v V, ttl time.Duration) bool
	GetTTL(k K) (time.Duration, bool)
	Close()
}

type CompressedCache[K comparable, V any] struct {
	c           *minder.Cache[K, []byte]
	compressor  compress.Compressor
	serialize   func(any) ([]byte, error)
	deserialize func([]byte) (any, error)
	mu          sync.RWMutex
}

func NewCompressedCache[K comparable, V any](
	cost int64,
	comp compress.Compressor,
	serialize func(any) ([]byte, error),
	deserialize func([]byte) (any, error),
) (Cache[K, V], error) {

	cache := minder.NewCache[K, []byte]()
	return &CompressedCache[K, V]{
		c:           cache,
		compressor:  comp,
		serialize:   serialize,
		deserialize: deserialize,
	}, nil
}

func (ch *CompressedCache[K, V]) Get(k K) (V, bool) {
	var zero V
	data, found := ch.c.Get(k)
	if !found {
		return zero, false
	}
	decompressed, err := ch.compressor.Decompress(data)
	if err != nil {
		return zero, false
	}
	value, err := ch.deserialize(decompressed)
	if err != nil {
		return zero, false
	}
	vTyped, ok := value.(V)
	if !ok {
		return zero, false
	}
	return vTyped, true

}

func (ch *CompressedCache[K, V]) GetTTL(k K) (time.Duration, bool) {
	ttl, found := ch.c.GetTTL(k)
	if !found {
		return 0, false
	}
	return ttl, true

}

func (ch *CompressedCache[K, V]) Set(k K, v V) bool {
	data, err := ch.serialize(v)
	if err != nil {
		return false
	}
	compressed, err := ch.compressor.Compress(data)
	if err != nil {
		return false
	}
	ok := ch.c.Set(k, compressed)

	return ok
}

func (ch *CompressedCache[K, V]) SetWithTTL(k K, v V, ttl time.Duration) bool {
	data, err := ch.serialize(v)
	if err != nil {
		return false
	}
	compressed, err := ch.compressor.Compress(data)
	if err != nil {
		return false
	}
	ok := ch.c.SetWithTTL(k, compressed, ttl)

	return ok
}

func (ch *CompressedCache[K, V]) Close() {
	ch.c.Close()
}

func (ch *CompressedCache[K, V]) Del(k K) {
	ch.c.Del(k)
}
