package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func main() {
	files, err := os.ReadDir("data")
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		fmt.Println("Scanning file: ", file.Name())
		metas, vals, err := handleBlock("data/" + file.Name())
		if err != nil {
			panic(err)
		}

		write := prompb.WriteRequest{
			Timeseries: vals,
			Metadata:   metas,
		}

		data, err := write.Marshal()
		if err != nil {
			panic(err)
		}

		f, err := os.Create("output/" + file.Name() + ".pb")
		if err != nil {
			panic(err)
		}

		defer f.Close()

		_, err = f.Write(data)
		if err != nil {
			panic(err)
		}
	}
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

	meta := []prompb.MetricMetadata{}
	for _, v := range metas {
		meta = append(meta, v)
	}

	return meta, series, nil
}
