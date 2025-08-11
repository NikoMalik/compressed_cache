package compressedcache

import (
	"sync"
	"time"

	"github.com/NikoMalik/compressed_cache/compress"
	sigil "github.com/NikoMalik/sigil"
)

type Cache[K sigil.Key, V any] interface {

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

type CompressedCache[K sigil.Key, V any] struct {
	c           *sigil.Cache[K, []byte]
	compressor  compress.Compressor
	serialize   func(any) ([]byte, error)
	deserialize func([]byte) (any, error)
	maxCost     int64
	numCounters int64
	mu          sync.RWMutex
}

func NewCompressedCache[K sigil.Key, V any](
	cost int64,
	comp compress.Compressor,
	serialize func(any) ([]byte, error),
	deserialize func([]byte) (any, error),
) (Cache[K, V], error) {
	numCounters := cost * 10 // 10x MaxCost

	cache, err := sigil.NewCache[K, []byte](&sigil.Config[K, []byte]{
		NumCounters: numCounters,
		MaxCost:     cost,
		BufferItems: 64,
		// OnEvict: func(item *sigil.Item[K, V]) {
		//       // Reload from DB
		//       v, err := loadFromDB(item.OriginalKey) load from db
		//       if err == nil {
		//           cache.SetWithTTL(item.OriginalKey, v, item.Cost, item.Expiration.Sub(time.Now()))
		//       }
		//   },
	})
	if err != nil {
		return nil, err
	}
	return &CompressedCache[K, V]{
		c:           cache,
		compressor:  comp,
		serialize:   serialize,
		deserialize: deserialize,
		maxCost:     cost,
		numCounters: numCounters,
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
	cost := int64(len(compressed))
	ok := ch.c.Set(k, compressed, cost)
	ch.c.Wait()

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
	cost := int64(len(compressed))
	ok := ch.c.SetWithTTL(k, compressed, cost, ttl)
	ch.c.Wait()

	return ok
}

func (ch *CompressedCache[K, V]) Close() {
	ch.c.Close()
}

func (ch *CompressedCache[K, V]) Del(k K) {
	ch.c.Del(k)
}
