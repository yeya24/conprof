package api

import (
	"context"
	"github.com/conprof/db/storage"
	"github.com/grafana/loki/pkg/loghttp"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/pkg/errors"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"net/http"
)

func (a *API) LokiQueryRange(r *http.Request) (interface{}, []error, *ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	request, err := loghttp.ParseRangeQuery(r)
	if err != nil {
		return nil, nil, &ApiError{Err: err}
	}

	params := logql.NewLiteralParams(
		request.Query,
		request.Start,
		request.End,
		request.Step,
		request.Interval,
		request.Direction,
		request.Limit,
		request.Shards,
	)
	query := a.logqlEngine.Query(params)
	result, err := query.Exec(r.Context())
	if err != nil {
		return nil, nil, &ApiError{Err: err}
	}

	return NewLokiResponseRenderer(result), nil, nil
}

func (a *API) lokiLabel(ctx context.Context, req *logproto.LabelRequest) (interface{}, []error, *ApiError) {
	q, err := a.db.Querier(ctx, timestamp.FromTime(*req.Start), timestamp.FromTime(*req.End))
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
	}

	if req.Name != "" {
		res, warnings, err := q.LabelValues(req.Name)
		if err != nil {
			return res, warnings, &ApiError{Err: err}
		}
		return res, warnings, nil
	}

	res, warnings, err := q.LabelNames()
	if err != nil {
		return res, warnings, &ApiError{Err: err}
	}
	return res, warnings, nil
}


func (a *API) LokiLabelNames(r *http.Request) (interface{}, []error, *ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	ctx := r.Context()

	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	}
	return a.lokiLabel(ctx, req)
}

func (a *API) LokiLabelValues(r *http.Request) (interface{}, []error, *ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	ctx := r.Context()

	req, err := loghttp.ParseLabelQuery(r)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	}
	req.Name = route.Param(ctx, "name")

	return a.lokiLabel(ctx, req)
}

func (a *API) LokiSeries(r *http.Request) (interface{}, []error, *ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	ctx := r.Context()

	req, err := loghttp.ParseSeriesQuery(r)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	}

	matchers, err := loghttp.Match(req.GetGroups())
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	}

	start := timestamp.FromTime(req.Start)
	end := timestamp.FromTime(req.End)
	q, err := a.db.Querier(ctx, start, end)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
	}

	var (
		metrics = []labels.Labels{}
		sets    []storage.SeriesSet
	)
	for _, mset := range matchers {
		sets = append(sets, q.Select(false, &storage.SelectHints{
			Start: start,
			End:   end,
			Func:  "series",
		}, mset...))
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
	}

	return metrics, nil, nil
}
