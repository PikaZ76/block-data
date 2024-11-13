package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

// Compressor 接口，定义了压缩和解压缩方法
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// CompressionType 定义了支持的压缩算法类型
type CompressionType string

const (
	Gzip   CompressionType = "gzip"
	Xz     CompressionType = "xz"
	Zstd   CompressionType = "zstd"
	Lz4    CompressionType = "lz4"
	Snappy CompressionType = "snappy"
	Brotli CompressionType = "brotli"
	Bzip2  CompressionType = "bzip2"
)

// CompressorFactory 工厂函数，根据算法名称和压缩级别返回对应的 Compressor
func CompressorFactory(compressionType CompressionType, level int) (Compressor, error) {
	switch compressionType {
	case Gzip:
		return NewGzipCompressor(level)
	case Xz:
		return NewXzCompressor()
	case Zstd:
		return NewZstdCompressor(level)
	case Lz4:
		return NewLz4Compressor(level)
	case Snappy:
		return NewSnappyCompressor()
	case Brotli:
		return NewBrotliCompressor(level)
	case Bzip2:
		return NewBzip2Compressor(level)
	default:
		return nil, errors.New("unsupported compression type")
	}
}

// GzipCompressor 实现了 gzip 压缩算法
type GzipCompressor struct {
	level int
}

func NewGzipCompressor(level int) (*GzipCompressor, error) {
	if level < 1 || level > 4 {
		return nil, errors.New("invalid gzip compression level")
	}
	return &GzipCompressor{level: level}, nil
}
func (c *GzipCompressor) mappingLevel() int {
	var gzipLevel int
	switch c.level {
	case 1:
		gzipLevel = gzip.BestSpeed
	case 2:
		gzipLevel = gzip.DefaultCompression
	case 3:
		gzipLevel = gzip.BestCompression
	case 4:
		gzipLevel = gzip.HuffmanOnly
	}
	return gzipLevel
}
func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, c.mappingLevel())
	if err != nil {
		return nil, errors.New("invalid gzip compression level")
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()
	return buf.Bytes(), nil
}

func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// SnappyCompressor 实现了 Snappy 压缩算法
type SnappyCompressor struct{}

func NewSnappyCompressor() (*SnappyCompressor, error) {
	return &SnappyCompressor{}, nil
}

func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// XzCompressor 实现了 xz 压缩算法
type XzCompressor struct{}

func NewXzCompressor() (*XzCompressor, error) {

	return &XzCompressor{}, nil
}

func (c *XzCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := xz.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()
	return buf.Bytes(), nil
}

func (c *XzCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := xz.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

// ZstdCompressor 实现了 zstd 压缩算法
type ZstdCompressor struct {
	level int // 1-5
}

func NewZstdCompressor(level int) (*ZstdCompressor, error) {
	if level < 1 || level > 4 {
		return nil, errors.New("invalid zstd compression level")
	}
	return &ZstdCompressor{level: level}, nil
}

func (c *ZstdCompressor) mappingLevel() zstd.EncoderLevel {
	var zstdLevel zstd.EncoderLevel
	switch c.level {
	case 1:
		zstdLevel = zstd.SpeedFastest
	case 2:
		zstdLevel = zstd.SpeedDefault
	case 3:
		zstdLevel = zstd.SpeedBetterCompression
	case 4:
		zstdLevel = zstd.SpeedBestCompression
	}
	return zstdLevel

}
func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(c.mappingLevel()))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func (c *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

// BrotliCompressor 实现了 Brotli 压缩算法
type BrotliCompressor struct {
	level int
}

func NewBrotliCompressor(level int) (*BrotliCompressor, error) {
	// 1: best speed
	// 7: default
	// 12: best compression
	brotliLevel := level - 1
	if brotliLevel < brotli.BestSpeed || brotliLevel > brotli.BestCompression {
		return nil, errors.New("invalid brotli compression level")
	}
	return &BrotliCompressor{level: level}, nil
}

func (c *BrotliCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := brotli.NewWriterLevel(&buf, c.level-1)
	_, err := writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()
	return buf.Bytes(), nil
}

func (c *BrotliCompressor) Decompress(data []byte) ([]byte, error) {
	reader := brotli.NewReader(bytes.NewReader(data))
	return io.ReadAll(reader)
}

// Bzip2Compressor 实现了 bzip2 压缩算法
type Bzip2Compressor struct {
	level int
}

func NewBzip2Compressor(level int) (*Bzip2Compressor, error) {
	// 1: best speed
	// 6: default middle
	// 9: best compression
	if level < bzip2.BestSpeed || level > bzip2.BestCompression {
		return nil, errors.New("invalid bzip2 compression level")
	}
	return &Bzip2Compressor{level: level}, nil
}

func (c *Bzip2Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := bzip2.NewWriter(&buf, &bzip2.WriterConfig{Level: c.level})
	if err != nil {
		return nil, err
	}
	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()
	return buf.Bytes(), nil
}

func (c *Bzip2Compressor) Decompress(data []byte) ([]byte, error) {
	reader, err := bzip2.NewReader(bytes.NewReader(data), nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// Lz4Compressor implements the LZ4 compression algorithm with support for compression levels 0-9.
type Lz4Compressor struct {
	level int
}

// NewLz4Compressor creates a new Lz4Compressor with the specified compression level (0-9).
func NewLz4Compressor(level int) (*Lz4Compressor, error) {
	// Validate the compression level.
	if level < 0 || level > 9 {
		return nil, fmt.Errorf("invalid lz4 compression level: %d (must be between 0 and 9)", level)
	}
	return &Lz4Compressor{level: level}, nil
}

func (c *Lz4Compressor) mappingLevel() lz4.CompressionLevel {
	// Map the level (0-9) to lz4.CompressionLevel constants.
	var lz4Level lz4.CompressionLevel
	switch c.level {
	case 0:
		lz4Level = lz4.Fast
	case 1:
		lz4Level = lz4.Level1
	case 2:
		lz4Level = lz4.Level2
	case 3:
		lz4Level = lz4.Level3
	case 4:
		lz4Level = lz4.Level4
	case 5:
		lz4Level = lz4.Level5
	case 6:
		lz4Level = lz4.Level6
	case 7:
		lz4Level = lz4.Level7
	case 8:
		lz4Level = lz4.Level8
	case 9:
		lz4Level = lz4.Level9
	default:
		// Should not reach here due to validation in NewLz4Compressor.
		lz4Level = lz4.Fast
	}
	// Apply the compression level option to the writer.
	return lz4Level
}

// Compress compresses the input data using the specified compression level.
func (c *Lz4Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	defer writer.Close() // Ensure the writer is closed even if an error occurs.

	// Set the compression level and check for errors.
	if err := writer.Apply(lz4.CompressionLevelOption(c.mappingLevel())); err != nil {
		return nil, err
	}
	// Write data to the writer.
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}

	// Close the writer to flush any remaining data.
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress decompresses the input data using LZ4.
func (c *Lz4Compressor) Decompress(data []byte) ([]byte, error) {
	// For decompression, no need to set the compression level.
	reader := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(reader)
}
