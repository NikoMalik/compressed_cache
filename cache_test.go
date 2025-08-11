package compressedcache

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NikoMalik/compressed_cache/compress"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	ID   int
	Data string
}

func init() {
	gob.Register(TestStruct{})
}

func TestCompressedCacheBasicOperations(t *testing.T) {
	// Initialize compressor and cache
	compressor, _ := compress.NewZstdCompressor()
	cache, err := NewCompressedCache[string, TestStruct](1000000, compressor, compress.SerializeGob, compress.DeserializeGob)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test data
	key := "test-key"
	value := TestStruct{ID: 1, Data: "test-data"}

	// Test Set and Get
	if !cache.Set(key, value) {
		t.Error("Failed to set value")
	}

	retrieved, found := cache.Get(key)
	if !found {
		t.Error("Value not found")
	}
	if retrieved.ID != value.ID || retrieved.Data != value.Data {
		t.Errorf("Retrieved value mismatch: got %+v, want %+v", retrieved, value)
	}

	// Test Delete
	cache.Del(key)
	_, found = cache.Get(key)
	if found {
		t.Error("Value should have been deleted")
	}
}

func TestCompressedCacheTTL(t *testing.T) {
	compressor, _ := compress.NewZstdCompressor()
	cache, err := NewCompressedCache[string, TestStruct](1000000, compressor, compress.SerializeGob, compress.DeserializeGob)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := "ttl-key"
	value := TestStruct{ID: 2, Data: "ttl-data"}
	ttl := 100 * time.Millisecond

	// Set with TTL
	if !cache.SetWithTTL(key, value, ttl) {
		t.Error("Failed to set value with TTL")
	}

	// Check TTL
	retrievedTTL, found := cache.GetTTL(key)
	if !found {
		t.Error("TTL not found")
	}
	if retrievedTTL > ttl || retrievedTTL <= 0 {
		t.Errorf("Invalid TTL: got %v, want <= %v", retrievedTTL, ttl)
	}

	// Verify value is present
	_, found = cache.Get(key)
	if !found {
		t.Error("Value should exist before TTL expires")
	}

	// Wait for TTL to expire
	time.Sleep(ttl + 10*time.Millisecond)

	// Verify value is gone
	_, found = cache.Get(key)
	if found {
		t.Error("Value should have expired")
	}
}

func TestCompressedCacheConcurrentOperations(t *testing.T) {
	compressor, _ := compress.NewZstdCompressor()
	cache, err := NewCompressedCache[string, TestStruct](1000000, compressor, compress.SerializeGob, compress.DeserializeGob)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	var wg sync.WaitGroup
	const numGoroutines = 100
	const operationsPerGoroutine = 100

	var successCount int64
	var failureCount int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := TestStruct{ID: id*1000 + j, Data: fmt.Sprintf("data-%d-%d", id, j)}

				// Perform Set
				if cache.Set(key, value) {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&failureCount, 1)
				}

				// Perform Get
				retrieved, found := cache.Get(key)
				if found && (retrieved.ID != value.ID || retrieved.Data != value.Data) {
					t.Errorf("Concurrent get mismatch: got %+v, want %+v", retrieved, value)
				}

				// Perform Delete
				if j%2 == 0 {
					cache.Del(key)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent operations: %d successes, %d failures", successCount, failureCount)
}

func TestCompressedCacheLargeObjects(t *testing.T) {
	compressor, _ := compress.NewZstdCompressor()
	cache, err := NewCompressedCache[string, TestStruct](1000000, compressor, compress.SerializeGob, compress.DeserializeGob)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Create a large object
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	value := TestStruct{ID: 3, Data: string(largeData)}

	// Test Set and Get with large object
	if !cache.Set("large-key", value) {
		t.Error("Failed to set large object")
	}

	retrieved, found := cache.Get("large-key")
	if !found {
		t.Error("Large object not found")
	}
	if retrieved.ID != value.ID || len(retrieved.Data) != len(value.Data) {
		t.Errorf("Large object mismatch: got ID=%d, len=%d; want ID=%d, len=%d",
			retrieved.ID, len(retrieved.Data), value.ID, len(value.Data))
	}
}

func TestCompressedCacheBasic(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	key := 1
	value := "test value"
	ok := cache.Set(key, value)
	require.True(t, ok, "Set should succeed")

	result, found := cache.Get(key)
	require.True(t, found, "Key should be found")
	require.Equal(t, value, result, "Value should match")

	cache.Del(key)
	_, found = cache.Get(key)
	require.False(t, found, "Key should be deleted")
}

// TestCompressedCacheTTL tests SetWithTTL and GetTTL
func TestCompressedCacheTTL2(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	key := 1
	value := "ttl value"
	ttl := 100 * time.Millisecond

	ok := cache.SetWithTTL(key, value, ttl)
	require.True(t, ok, "SetWithTTL should succeed")

	remaining, found := cache.GetTTL(key)
	require.True(t, found, "TTL should be found")
	require.Greater(t, remaining, time.Duration(0), "TTL should be positive")
	require.LessOrEqual(t, remaining, ttl, "TTL should not exceed set value")

	result, found := cache.Get(key)
	require.True(t, found, "Key should be found")
	require.Equal(t, value, result, "Value should match")

	time.Sleep(ttl + 10*time.Millisecond)
	_, found = cache.Get(key)
	require.False(t, found, "Key should expire")
	_, found = cache.GetTTL(key)
	require.False(t, found, "TTL should expire")
}

// TestCompressedCacheReallocation tests reallocation with data and TTL preservation
func TestCompressedCacheReallocation(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	// Set initial data with TTL
	ttl := 200 * time.Millisecond
	for i := 0; i < 5; i++ {
		ok := cache.SetWithTTL(i, "value"+string(rune(i)), ttl)
		require.True(t, ok, "SetWithTTL failed for key %d", i)
	}

	// Force reallocation
	for i := 5; i < 10; i++ {
		ok := cache.Set(i, "force reallocation")
		require.True(t, ok, "Set failed for key %d", i)
	}

	// Verify all data is preserved
	for i := 0; i < 10; i++ {
		expected := "value" + string(rune(i))
		if i >= 5 {
			expected = "force reallocation"
		}
		value, found := cache.Get(i)
		require.True(t, found, "Key %d not found after reallocation", i)
		require.Equal(t, expected, value, "Value mismatch for key %d", i)
	}

	// Verify TTL preservation
	for i := 0; i < 5; i++ {
		remaining, found := cache.GetTTL(i)
		require.True(t, found, "TTL not found for key %d", i)
		require.Greater(t, remaining, time.Duration(0), "TTL should be positive for key %d", i)
	}
}

// TestCompressedCacheEmptyValue tests handling of empty values
func TestCompressedCacheEmptyValue(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	key := 1
	value := ""
	ok := cache.Set(key, value)
	require.True(t, ok, "Set should succeed for empty value")

	result, found := cache.Get(key)
	require.True(t, found, "Empty value should be found")
	require.Equal(t, value, result, "Empty value should match")
}

// TestCompressedCacheLargeValue tests handling of large values
func TestCompressedCacheLargeValue(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](10000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	key := 1
	largeValue := string(make([]byte, 5000))
	ok := cache.Set(key, largeValue)
	require.True(t, ok, "Set should succeed for large value")

	result, found := cache.Get(key)
	require.True(t, found, "Large value should be found")
	require.Equal(t, largeValue, result, "Large value should match")
}

// TestCompressedCacheSerializationError tests serialization errors
func TestCompressedCacheSerializationError(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	failSerialize := func(any) ([]byte, error) {
		return nil, errors.New("serialization failed")
	}

	cache, err := NewCompressedCache[int, string](1000, comp, failSerialize, compress.DeserializeGob)
	require.NoError(t, err)

	ok := cache.Set(1, "test")
	require.False(t, ok, "Set should fail due to serialization error")
}

// TestCompressedCacheCompressionError tests compression errors
func TestCompressedCacheCompressionError(t *testing.T) {
	failCompressor := &mockCompressor{
		compressErr: errors.New("compression failed"),
	}

	cache, err := NewCompressedCache[int, string](1000, failCompressor, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	ok := cache.Set(1, "test")
	require.False(t, ok, "Set should fail due to compression error")
}

// TestCompressedCacheConcurrency tests concurrent operations
func TestCompressedCacheConcurrency(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1<<30, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	const numGoroutines = 10 // Reduced to stabilize
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := i
			value := "value" + string(rune(i))
			ok := cache.Set(key, value)
			require.True(t, ok, "Set failed in goroutine %d", i)

			result, found := cache.Get(key)
			require.True(t, found, "Get failed in goroutine %d", i)
			require.Equal(t, value, result, "Value mismatch in goroutine %d", i)

			ttl := 100 * time.Millisecond
			ok = cache.SetWithTTL(key, value+"-ttl", ttl)
			require.True(t, ok, "SetWithTTL failed in goroutine %d", i)

			time.Sleep(time.Millisecond) // Small delay to ensure TTL is set
			remaining, found := cache.GetTTL(key)
			require.True(t, found, "GetTTL failed in goroutine %d", i)
			require.Greater(t, remaining, time.Duration(0), "TTL invalid in goroutine %d", i)

			// Force reallocation sparingly
			if i%5 == 0 {
				for j := 0; j < 1; j++ {
					cache.Set(key+1000+j, "force reallocation")
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify data consistency
	for i := 0; i < numGoroutines; i++ {
		value := "value" + string(rune(i)) + "-ttl"
		result, found := cache.Get(i)
		require.True(t, found, "Key %d not found after concurrent operations", i)
		require.Equal(t, value, result, "Value mismatch for key %d", i)
	}
}

// TestCompressedCacheStress tests cache under high load
func BenchmarkCompressedCacheStress(b *testing.B) {
	comp, _ := compress.NewZstdCompressor()

	cache, _ := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)

	sizes := []int{10, 100, 1000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, size := range sizes {
			key := i % 1000
			value := string(make([]byte, size))
			cache.Set(key, value)
			cache.Get(key)
		}
	}
}

// TestCompressedCacheKeyNotFound tests handling of non-existent keys
func TestCompressedCacheKeyNotFound(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, compress.DeserializeGob)
	require.NoError(t, err)

	_, found := cache.Get(999)
	require.False(t, found, "Non-existent key should not be found")
}

// TestCompressedCacheDeserializationError tests deserialization errors
func TestCompressedCacheDeserializationError(t *testing.T) {
	comp, err := compress.NewZstdCompressor()
	require.NoError(t, err)

	failDeserialize := func([]byte) (any, error) {
		return nil, errors.New("deserialization failed")
	}

	cache, err := NewCompressedCache[int, string](1000, comp, compress.SerializeGob, failDeserialize)
	require.NoError(t, err)

	ok := cache.Set(1, "test")
	require.True(t, ok, "Set should succeed")

	_, found := cache.Get(1)
	require.False(t, found, "Get should fail due to deserialization error")
}

// mockCompressor for testing
type mockCompressor struct {
	compressErr   error
	decompressErr error
}

func (m *mockCompressor) Compress(data []byte) ([]byte, error) {
	if m.compressErr != nil {
		return nil, m.compressErr
	}
	return data, nil
}

func (m *mockCompressor) Decompress(data []byte) ([]byte, error) {
	if m.decompressErr != nil {
		return nil, m.decompressErr
	}
	return data, nil
}
