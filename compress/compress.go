package compress

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := new(bytes.Buffer)
		b.Grow(1024 * 30)
		return b
	},
}

var _ Compressor = (*ZstdCompressor)(nil)

func SerializeGob(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeGob(data []byte) (interface{}, error) {
	var v interface{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

type ZstdCompressor struct {
	encPoolFast    sync.Pool
	encPoolDefault sync.Pool
	decPool        sync.Pool
}

func NewZstdCompressor() (*ZstdCompressor, error) {
	return &ZstdCompressor{
		encPoolFast: sync.Pool{
			New: func() interface{} {
				enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
				if err != nil {
					panic(err)
				}
				return enc
			},
		},
		encPoolDefault: sync.Pool{
			New: func() interface{} {
				enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
				if err != nil {
					panic(err)
				}
				return enc
			},
		},
		decPool: sync.Pool{
			New: func() interface{} {
				dec, err := zstd.NewReader(nil)
				if err != nil {
					panic(err)
				}
				return dec
			},
		},
	}, nil
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	// Skip compression for very small data (<100 bytes) to avoid overhead
	if len(data) < 100 {
		return append([]byte{}, data...), nil
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Choose encoder based on data size
	encPool := &z.encPoolFast
	if len(data) > 1024 { // Use SpeedDefault for larger data
		encPool = &z.encPoolDefault
	}

	enc := encPool.Get().(*zstd.Encoder)
	defer encPool.Put(enc)

	enc.Reset(buf)
	if _, err := enc.Write(data); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	// If data is uncompressed (small data), return as is
	if len(data) < 100 && !z.IsCompressed(data) {
		return append([]byte{}, data...), nil
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	dec := z.decPool.Get().(*zstd.Decoder)
	defer z.decPool.Put(dec)

	if err := dec.Reset(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	if _, err := io.Copy(buf, dec); err != nil {
		return nil, err
	}
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// IsCompressed checks if data is zstd-compressed (simplified check)
func (z *ZstdCompressor) IsCompressed(data []byte) bool {
	return len(data) >= 4 && data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}
