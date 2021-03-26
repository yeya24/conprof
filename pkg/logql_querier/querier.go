package logql_querier

import (
	"bytes"
	"context"
	"fmt"
	"github.com/conprof/conprof/internal/pprof/report"
	"github.com/conprof/db/storage"
	"github.com/google/pprof/profile"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/prometheus/prometheus/pkg/labels"
	"strings"
	"time"
)

type Querier struct {
	db storage.Queryable
}

func NewQuerier(db storage.Queryable) *Querier {
	return &Querier{db: db}
}

func (q *Querier) SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error) {
	sel, err := params.LogSelector()
	if err != nil {
		return nil, err
	}

	// We just ignore the pipe for now.
	pipeline, err := sel.Pipeline()
	if err != nil {
		return nil, err
	}
	_ = pipeline

	querier, err := q.db.Querier(ctx, params.Start.Unix()*1000, params.End.Unix()*1000)
	if err != nil {
		return nil, err
	}

	ss := querier.Select(false, nil, sel.Matchers()...)
	streams := []logproto.Stream{}
	for ss.Next() {
		s := ss.At()
		profileLabels := s.Labels()
		i := s.Iterator()

		for i.Next() {
			t, profileBytes := i.At()
			p, err := profile.Parse(bytes.NewBuffer(profileBytes))
			if err != nil {
				// continue as a non parseable profile should never make it into the database
				continue
			}

			r := report.NewDefault(p, report.Options{})
			ti, ls := report.TextItems(r)
			newLbls := profileLabels.Copy()
			for _, l := range ls {
				if strings.HasPrefix(l, "Type:") {
					newLbls = append(newLbls, labels.Label{Name: "type", Value: l[5:]})
				}
			}
			stream := logproto.Stream{
				Labels: newLbls.String(),
			}

			for _, item := range ti {
				stream.Entries = append(stream.Entries, logproto.Entry{
					Timestamp: time.Unix(t/1000, 0),
					Line:      item.Name + fmt.Sprintf(" cum=%d", item.Cum),
				})
			}

			streams = append(streams, stream)
		}
	}

	if err := ss.Err(); err != nil {
		return nil, err
	}

	return iter.NewStreamsIterator(ctx, streams, logproto.FORWARD), nil
}

func (q *Querier) SelectSamples(context.Context, logql.SelectSampleParams) (iter.SampleIterator, error) {
	return nil, nil
}
