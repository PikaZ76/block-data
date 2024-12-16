package main

import (
	"compression-project/compression"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var algorithmsWithLevels = map[string][]int{
	"gzip":   {1, 2},
	// "xz":     {0},
	"snappy": {0},
	"zstd":   {1, 2, 3, 4},
	"zstdD":   {1, 2, 3, 4},
	// "brotli": {1, 2, 3, 4, 5, 6, 7, 8}, // 12, too slow this level ......
	// "bzip2":  {1, 6, 9},
	// "lz4":    {0, 1, 2, 3, 4, 5, 9},
}

func getJSONFiles(inputDir string) ([]string, error) {
	if inputDir == "" {
		return nil, fmt.Errorf("input directory is not specified")
	}
	files, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}
	var jsonFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			jsonFiles = append(jsonFiles, filepath.Join(inputDir, file.Name()))
		}
	}
	if len(jsonFiles) == 0 {
		return nil, fmt.Errorf("no JSON files found in the specified input directory")
	}
	return jsonFiles, nil
}

func setupEnvironment() (tempDir string, err error) {
	tempDir = "temp"
	err = os.MkdirAll(tempDir, os.ModePerm)
	return
}

func getExtension(algorithm string) string {
	switch algorithm {
	case "gzip":
		return ".gz"
	case "xz":
		return ".xz"
	case "zstd", "zstdD":
		return ".zst"
	case "lz4":
		return ".lz4"
	case "snappy":
		return ".snappy"
	case "brotli":
		return ".br"
	case "bzip2":
		return ".bz2"
	default:
		return ".compressed"
	}
}

func parseFlags() (inputDir string, epochs int, zstdDictPath string) {
	flag.StringVar(&inputDir, "input", "", "Directory containing JSON files to compress")
	flag.IntVar(&epochs, "epoch", 1, "Number of times to repeat compression and decompression")
	flag.StringVar(&zstdDictPath, "zstd_dict", "", "Path to zstd dictionary file (optional)")
	flag.Parse()
	return
}

type AlgorithmBenchmark struct {
	algorithm         string
	level             int
	dataMap           map[string][]byte
	totalOriginalSize int64
	epochs            int
	tempDir           string
	statsFile         *os.File
	compressor        compression.Compressor
}

func NewAlgorithmBenchmark(algorithm string, level int, dictMap map[compression.CompressionType][]byte, dataMap map[string][]byte, totalOriginalSize int64, epochs int, tempDir string, statsFile *os.File) (*AlgorithmBenchmark, error) {
	compressor, err := compression.CompressorFactory(compression.CompressionType(algorithm), level, dictMap)
	if err != nil {
		return nil, err
	}
	return &AlgorithmBenchmark{
		algorithm:         algorithm,
		level:             level,
		dataMap:           dataMap,
		totalOriginalSize: totalOriginalSize,
		epochs:            epochs,
		tempDir:           tempDir,
		statsFile:         statsFile,
		compressor:        compressor,
	}, nil
}

func (ab *AlgorithmBenchmark) Run() {
	err := ab.runBenchmark()
	if err != nil {
		fmt.Printf("Error running benchmark for %s level %d: %v\n", ab.algorithm, ab.level, err)
	}
}

func (ab *AlgorithmBenchmark) createLogFile() (*os.File, error) {
	logFileName := fmt.Sprintf("benchmark_%s_level_%d.log", ab.algorithm, ab.level)
	logFilePath := filepath.Join(ab.tempDir, logFileName)
	return os.Create(logFilePath)
}

func (ab *AlgorithmBenchmark) logInitialInfo(logFile *os.File) {
	fmt.Fprintf(logFile, "Compression Algorithm: %s\n", ab.algorithm)
	fmt.Fprintf(logFile, "Compression Level: %d\n", ab.level)
	fmt.Fprintf(logFile, "Number of Source Files: %d\n", len(ab.dataMap))
	fmt.Fprintf(logFile, "Epochs: %d\n", ab.epochs)
	fmt.Fprintln(logFile, "----------------------------------------")

	for fileName, data := range ab.dataMap {
		fmt.Fprintf(logFile, "File: %s, Size: %d bytes\n", fileName, len(data))
	}
	fmt.Fprintln(logFile, "----------------------------------------")
}

func (ab *AlgorithmBenchmark) runBenchmark() error {
	logFile, err := ab.createLogFile()
	if err != nil {
		return err
	}
	defer logFile.Close()

	ab.logInitialInfo(logFile)

	ext := getExtension(ab.algorithm)
	totalCompressTime := time.Duration(0)
	totalDecompressTime := time.Duration(0)
	var compressionRatio float64

	for epoch := 1; epoch <= ab.epochs; epoch++ {
		fmt.Printf("Algorithm: %s, Level: %d, Epoch %d/%d\n", ab.algorithm, ab.level, epoch, ab.epochs)

		compressTime, decompressTime, compressedSize, err := ab.processEpoch(epoch, ext, logFile)
		if err != nil {
			fmt.Printf("Error in epoch %d: %v\n", epoch, err)
			continue
		}

		totalCompressTime += compressTime
		totalDecompressTime += decompressTime

		compressionRatio = float64(compressedSize) / float64(ab.totalOriginalSize) * 100

		ab.logEpochSummary(logFile, epoch, compressTime, decompressTime, compressionRatio)
	}

	avgCompressTime := totalCompressTime / time.Duration(ab.epochs)
	avgDecompressTime := totalDecompressTime / time.Duration(ab.epochs)
	ab.outputSummary(logFile, avgCompressTime, avgDecompressTime, compressionRatio)

	fmt.Fprintf(ab.statsFile, "%s,%d,average,%.2f,%.2f,%.2f\n",
		ab.algorithm, ab.level, avgCompressTime.Seconds()*1000, avgDecompressTime.Seconds()*1000, compressionRatio)

	return nil
}

func (ab *AlgorithmBenchmark) processEpoch(epoch int, ext string, logFile *os.File) (compressTime, decompressTime time.Duration, compressedSize int64, err error) {
	epochDir := filepath.Join(ab.tempDir, fmt.Sprintf("benchmark_%s", ab.algorithm), fmt.Sprintf("%d", epoch))
	compressDir := filepath.Join(epochDir, "compress")
	os.MkdirAll(compressDir, os.ModePerm)

	for fileName, data := range ab.dataMap {
		compressedData, cTime, err := ab.compressData(data)
		if err != nil {
			fmt.Printf("Error compressing file %s: %v\n", fileName, err)
			continue
		}
		outputFileName := strings.TrimSuffix(fileName, ".json") + ext
		outputFilePath := filepath.Join(compressDir, outputFileName)
		err = os.WriteFile(outputFilePath, compressedData, 0644)
		if err != nil {
			fmt.Printf("Error writing compressed file %s: %v\n", outputFilePath, err)
			continue
		}
		compressTime += cTime
		compressedSize += int64(len(compressedData))

		fmt.Printf("Compressed %s: Compressed Size=%d bytes, Time=%v\n",
			fileName, len(compressedData), cTime)
		fmt.Fprintf(logFile, "Epoch %d, Compressed %s: Size=%d bytes, Time=%v\n",
			epoch, fileName, len(compressedData), cTime)
	}

	for fileName := range ab.dataMap {
		compressedFileName := strings.TrimSuffix(fileName, ".json") + ext
		compressedFilePath := filepath.Join(compressDir, compressedFileName)

		dTime, err := ab.decompressData(compressedFilePath)
		if err != nil {
			fmt.Printf("Error decompressing file %s: %v\n", compressedFilePath, err)
			continue
		}
		decompressTime += dTime

		fmt.Printf("Decompressed %s: Time=%v\n", compressedFileName, dTime)
		fmt.Fprintf(logFile, "Epoch %d, Decompressed %s: Time=%v\n",
			epoch, compressedFileName, dTime)
	}

	return
}

func (ab *AlgorithmBenchmark) compressData(data []byte) ([]byte, time.Duration, error) {
	startTime := time.Now()
	compressedData, err := ab.compressor.Compress(data)
	duration := time.Since(startTime)
	return compressedData, duration, err
}

func (ab *AlgorithmBenchmark) decompressData(compressedFilePath string) (time.Duration, error) {
	compressedData, err := os.ReadFile(compressedFilePath)
	if err != nil {
		return 0, err
	}
	startTime := time.Now()
	_, err = ab.compressor.Decompress(compressedData)
	duration := time.Since(startTime)
	return duration, err
}

func (ab *AlgorithmBenchmark) logEpochSummary(logFile *os.File, epoch int, compressTime, decompressTime time.Duration, compressionRatio float64) {
	fmt.Fprintf(logFile, "Epoch %d Summary:\n", epoch)
	fmt.Fprintf(logFile, "  Compression Time: %v\n", compressTime)
	fmt.Fprintf(logFile, "  Decompression Time: %v\n", decompressTime)
	fmt.Fprintf(logFile, "  Compression Ratio: %.2f%%\n", compressionRatio)
	fmt.Fprintln(logFile, "----------------------------------------")

	fmt.Fprintf(ab.statsFile, "%s,%d,%d,%.2f,%.2f,%.2f\n",
		ab.algorithm, ab.level, epoch, compressTime.Seconds()*1000, decompressTime.Seconds()*1000, compressionRatio)
}

func (ab *AlgorithmBenchmark) outputSummary(logFile *os.File, avgCompressTime, avgDecompressTime time.Duration, avgCompressionRatio float64) {
	fmt.Println("Overall Summary:")
	fmt.Printf("  Average Compression Time per Epoch: %v\n", avgCompressTime)
	fmt.Printf("  Average Decompression Time per Epoch: %v\n", avgDecompressTime)
	fmt.Printf("  Average Compression Ratio: %.2f%%\n", avgCompressionRatio)

	fmt.Fprintln(logFile, "Overall Summary:")
	fmt.Fprintf(logFile, "  Average Compression Time: %v\n", avgCompressTime)
	fmt.Fprintf(logFile, "  Average Decompression Time: %v\n", avgDecompressTime)
	fmt.Fprintf(logFile, "  Average Compression Ratio: %.2f%%\n", avgCompressionRatio)
}

func preloadData(jsonFiles []string) (map[string][]byte, int64, error) {
	dataMap := make(map[string][]byte)
	var totalOriginalSize int64
	for _, filePath := range jsonFiles {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, 0, fmt.Errorf("error reading file %s: %v", filePath, err)
		}
		fileName := filepath.Base(filePath)
		dataMap[fileName] = data
		totalOriginalSize += int64(len(data))
	}
	return dataMap, totalOriginalSize, nil
}

func main() {
	inputDir, epochs, zstdDictPath := parseFlags()

	jsonFiles, err := getJSONFiles(inputDir)
	if err != nil {
		fmt.Printf("Error getting JSON files: %v\n", err)
		return
	}

	tempDir, err := setupEnvironment()
	if err != nil {
		fmt.Printf("Error setting up environment: %v\n", err)
		return
	}

	dataMap, totalOriginalSize, err := preloadData(jsonFiles)
	if err != nil {
		fmt.Printf("Error preloading data: %v\n", err)
		return
	}
	// 加载 zstd 字典（如果提供）
    var zstdDict []byte
    if zstdDictPath != "" {
        zstdDict, err = os.ReadFile(zstdDictPath)
        if err != nil {
            fmt.Printf("Error reading zstd dictionary file: %v\n", err)
            return
        }
        fmt.Printf("Loaded zstd dictionary from %s, size: %d bytes\n", zstdDictPath, len(zstdDict))
    }

    // 创建字典映射
    dictionaries := make(map[compression.CompressionType][]byte)
    if zstdDict != nil {
        dictionaries[compression.Zstd] = zstdDict
    }
	statsFile, err := os.Create(filepath.Join(tempDir, "benchmark_stats.csv"))
	if err != nil {
		fmt.Printf("Error creating stats file: %v\n", err)
		return
	}
	defer statsFile.Close()

	fmt.Fprintf(statsFile, "Algorithm,Level,Epoch,CompressionTime(ms),DecompressionTime(ms),CompressionRatio(%%)\n")

	for algorithm, levels := range algorithmsWithLevels {
		for _, level := range levels {
			ab, err := NewAlgorithmBenchmark(algorithm, level, dictionaries, dataMap, totalOriginalSize, epochs, tempDir, statsFile)
			if err != nil {
				fmt.Printf("Error creating benchmark for %s level %d: %v\n", algorithm, level, err)
				continue
			}
			ab.Run()
		}
	}
}
