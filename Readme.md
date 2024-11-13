# run benchmark




```
cd compress-project
go mod tidy
go run benchmark.go --input ../eth_blocks --epoch 5
```
This will run all compression algorithms with each level (if supported) for epoch rounds, then generate logs and a final status csv file inside the ./temp used for analysis later.

this benchmark used 27 Ethereum blocks inside the eth_blocks as test dataset.
```
	"gzip":   {1, 2, 3, 4},
	"xz":     {0},
	"snappy": {0},
	"zstd":   {1, 2, 3, 4},
	"brotli": {1, 2, 3, 4, 5, 6, 7, 8},
	"bzip2":  {1, 6, 9},
	"lz4":    {0, 1, 2, 3, 4, 5, 9},
```

# Analysis
used this jupyter notebook:
[bubble_polit.ipynb](https://github.com/PikaZ76/block-data/blob/main/compress-project/bubble_plot.ipynb)

it's will generate a bubble chart.

