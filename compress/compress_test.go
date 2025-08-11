package compress

import (
	"encoding/gob"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZstdCompressor(t *testing.T) {
	comp, _ := NewZstdCompressor()

	data := []byte("test data")
	compressed, err := comp.Compress(data)
	require.NoError(t, err, "Compress should succeed")
	assert.NotEmpty(t, compressed, "Compressed data should not be empty")

	decompressed, err := comp.Decompress(compressed)
	require.NoError(t, err, "Decompress should succeed")
	assert.Equal(t, data, decompressed, "Decompressed data should match original")
}

func TestZstdCompressorWithGob(t *testing.T) {
	comp, err := NewZstdCompressor()
	require.NoError(t, err)

	type testStruct struct {
		ID   int
		Name string
	}
	gob.Register(testStruct{})
	original := testStruct{ID: 123, Name: "Test Data"}

	// ser
	serialized, err := SerializeGob(original)
	require.NoError(t, err)

	// compress
	compressed, err := comp.Compress(serialized)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)
	// Relaxed size check: allow compressed size to be slightly larger for small data
	require.LessOrEqual(t, len(compressed), len(serialized)*2, "Compressed data should not be excessively large")

	//dec
	decompressed, err := comp.Decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, serialized, decompressed)

	//des
	deserialized, err := DeserializeGob(decompressed)
	require.NoError(t, err)
	result, ok := deserialized.(testStruct)
	require.True(t, ok)
	require.Equal(t, original, result)
}

func BenchmarkZstdCompressor(b *testing.B) {
	comp, err := NewZstdCompressor()
	require.NoError(b, err)

	//  (1MB)
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := comp.Compress(data)
		require.NoError(b, err)
	}
}

func TestZstdCompressorConcurrency(t *testing.T) {
	comp, err := NewZstdCompressor()
	require.NoError(t, err)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	data := []byte("test data for concurrent compression")
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			compressed, err := comp.Compress(data)
			require.NoError(t, err, "Compression failed in goroutine %d", i)
			decompressed, err := comp.Decompress(compressed)
			require.NoError(t, err, "Decompression failed in goroutine %d", i)
			require.Equal(t, data, decompressed, "Data mismatch in goroutine %d", i)
		}(i)
	}
	wg.Wait()
}

func TestZstdCompressorSmallData(t *testing.T) {
	comp, err := NewZstdCompressor()
	require.NoError(t, err)

	data := []byte("small")
	compressed, err := comp.Compress(data)
	require.NoError(t, err)
	require.Equal(t, data, compressed, "Small data should not be compressed")

	decompressed, err := comp.Decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, data, decompressed)
}
