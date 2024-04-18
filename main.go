package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var CLI struct {
	Serialize struct {
		BlockDir  string `arg:"" help:"Directory containing the block to serialize." required:""`
		OutputDir string `arg:"" help:"Directory to write the serialized protobuf files to." required:""`
	} `cmd:"serialize" help:"Serialize a set of blocks to protobuf files."`
	Replay struct {
		InputDir       string `arg:"" help:"Directory containing the protobuf files to replay." required:""`
		OutputEndpoint string `arg:"" help:"Endpoint to send the replayed data to." required:""`
	} `cmd:"replay" help:"Replay a set of protobuf files to a remote endpoint."`
}

func main() {
	ctx := kong.Parse(&CLI)

	switch ctx.Command() {
	case "serialize <block-dir> <output-dir>":
		err := serialize(CLI.Serialize.BlockDir, CLI.Serialize.OutputDir)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "replay <input-dir> <output-endpoint>":
		if err := replay(CLI.Replay.InputDir, CLI.Replay.OutputEndpoint); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	default:
		fmt.Println("Invalid command: ", ctx.Command())
		os.Exit(1)
	}
}

func replay(inputDir, outputEndpoint string) error {
	files, err := os.ReadDir(inputDir)
	if err != nil {
		return fmt.Errorf("error reading input directory: %w", err)
	}

	fmt.Println("Replaying to endpoint: ", outputEndpoint)

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".pb") {
			continue
		}

		fmt.Println("Replaying file: ", file.Name())
		data, err := os.ReadFile(inputDir + "/" + file.Name())
		if err != nil {
			return fmt.Errorf("error reading file: %w", err)
		}

		data = snappy.Encode(nil, data)

		request, err := http.NewRequest(http.MethodPost, outputEndpoint, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("error creating request: %w", err)
		}

		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("Content-Encoding", "snappy")

		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			return fmt.Errorf("error sending data to endpoint: %w", err)
		}

		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			body := bytes.Buffer{}
			_, err := body.ReadFrom(resp.Body)
			if err != nil {
				return fmt.Errorf("error reading response body: %w", err)
			}

			return fmt.Errorf("error response from endpoint: %d: %s", resp.StatusCode, body.String())
		}
	}

	return nil
}

func serialize(dataDir, outputDir string) error {
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.Mkdir(outputDir, 0755); err != nil {
			return fmt.Errorf("error creating output directory: %w", err)
		}
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		if !file.IsDir() || len(file.Name()) != 26 {
			continue
		}

		fmt.Println("Scanning file: ", file.Name())
		metas, vals, err := handleBlock(dataDir + "/" + file.Name())
		if err != nil {
			return fmt.Errorf("error handling block: %w", err)
		}

		write := prompb.WriteRequest{
			Timeseries: vals,
			Metadata:   metas,
		}

		data, err := write.Marshal()
		if err != nil {
			return fmt.Errorf("error marshalling write request: %w", err)
		}

		f, err := os.Create(outputDir + "/" + file.Name() + ".pb")
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}

		defer f.Close()

		_, err = f.Write(data)
		if err != nil {
			return fmt.Errorf("error writing data to file: %w", err)
		}
	}

	return nil
}

func handleBlock(blockPath string) ([]prompb.MetricMetadata, []prompb.TimeSeries, error) {
	ctx := context.Background()
	block, err := tsdb.OpenBlock(nil, blockPath, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening block %s: %w", blockPath, err)
	}

	ir, err := block.Index()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting index reader: %w", err)
	}

	p, err := ir.Postings(ctx, "", "")
	if err != nil {
		return nil, nil, fmt.Errorf("error getting postings: %w", err)
	}

	chks := []chunks.Meta{}
	builder := labels.ScratchBuilder{}

	labelMap := map[string][]prompb.Label{}
	batch := map[string][]prompb.Sample{}
	metas := map[string]prompb.MetricMetadata{}
	for p.Next() {
		if err = ir.Series(p.At(), &builder, &chks); err != nil {
			return nil, nil, fmt.Errorf("error getting series: %w", err)
		}

		reader, err := block.Chunks()
		if err != nil {
			return nil, nil, fmt.Errorf("error getting chunk reader: %w", err)
		}

		pbLabels := []prompb.Label{}
		name := builder.Labels().Get(model.MetricNameLabel)
		for _, l := range builder.Labels() {
			pbLabels = append(pbLabels, prompb.Label{Name: l.Name, Value: l.Value})
		}

		labelKey := builder.Labels().String()
		labelMap[labelKey] = pbLabels

		for _, chk := range chks {
			chunk, _, err := reader.ChunkOrIterable(chk)
			if err != nil {
				return nil, nil, fmt.Errorf("error getting chunk or iterable: %w", err)
			}

			cIter := chunk.Iterator(nil)
			nxt := cIter.Next()
			meta := prompb.MetricMetadata{}
			for nxt != chunkenc.ValNone {
				if nxt == chunkenc.ValFloat {
					if strings.HasSuffix(name, "_total") {
						meta.Type = prompb.MetricMetadata_COUNTER
					} else {
						meta.Type = prompb.MetricMetadata_GAUGE
					}

					meta.MetricFamilyName = name
				} else if nxt == chunkenc.ValHistogram {
					meta.Type = prompb.MetricMetadata_HISTOGRAM
					meta.MetricFamilyName = strings.TrimSuffix(name, "_bucket")
				} else if nxt == chunkenc.ValFloatHistogram {
					meta.Type = prompb.MetricMetadata_SUMMARY
					meta.MetricFamilyName = strings.TrimSuffix(name, "_bucket")
				}

				t, v := cIter.At()
				batch[labelKey] = append(batch[labelKey], prompb.Sample{Timestamp: t, Value: v})
				nxt = cIter.Next()

				metas[labelKey] = meta
			}
		}
	}

	series := []prompb.TimeSeries{}
	for k, v := range batch {
		series = append(series, prompb.TimeSeries{Labels: labelMap[k], Samples: v})
	}

	fmt.Println("Found", len(series))

	meta := []prompb.MetricMetadata{}
	for _, v := range metas {
		meta = append(meta, v)
	}

	return meta, series, nil
}
