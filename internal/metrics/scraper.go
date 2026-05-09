package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Signals struct {
	Service    string
	PodIP      string
	LatencyMs  float64
	ErrorRate  float64
	CPUPercent float64
}

type Scraper struct {
	addr   string
	client *http.Client
}

func NewScraper(addr string) *Scraper {
	return &Scraper{
		addr:   addr,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *Scraper) FetchSignals(ctx context.Context) ([]Signals, error) {
	latency, err := s.query(ctx, `rate(envoy_upstream_rq_time_sum[1m]) / rate(envoy_upstream_rq_time_count[1m])`)
	if err != nil {
		return nil, fmt.Errorf("latency query: %w", err)
	}
	errRate, err := s.query(ctx, `rate(envoy_upstream_rq_xx{response_code_class="5"}[1m])`)
	if err != nil {
		return nil, fmt.Errorf("error rate query: %w", err)
	}
	cpu, err := s.query(ctx, `rate(container_cpu_usage_seconds_total[1m]) * 100`)
	if err != nil {
		return nil, fmt.Errorf("cpu query: %w", err)
	}

	signals := map[string]*Signals{}
	for _, r := range latency {
		if _, ok := signals[r.IP]; !ok {
			signals[r.IP] = &Signals{PodIP: r.IP}
		}
		signals[r.IP].LatencyMs = r.Value
	}
	for _, r := range errRate {
		if _, ok := signals[r.IP]; !ok {
			signals[r.IP] = &Signals{PodIP: r.IP}
		}
		signals[r.IP].ErrorRate = r.Value
	}
	for _, r := range cpu {
		if _, ok := signals[r.IP]; !ok {
			signals[r.IP] = &Signals{PodIP: r.IP}
		}
		signals[r.IP].CPUPercent = r.Value
	}

	out := make([]Signals, 0, len(signals))
	for _, v := range signals {
		out = append(out, *v)
	}
	return out, nil
}

type queryResult struct {
	IP    string
	Value float64
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]interface{}    `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (s *Scraper) query(ctx context.Context, q string) ([]queryResult, error) {
	params := url.Values{}
	params.Set("query", q)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/v1/query?%s", s.addr, params.Encode()), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr promResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}

	var results []queryResult
	for _, r := range pr.Data.Result {
		ip := r.Metric["instance"]
		for i, c := range ip {
			if c == ':' {
				ip = ip[:i]
				break
			}
		}
		valStr, ok := r.Value[1].(string)
		if !ok {
			continue
		}
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		results = append(results, queryResult{IP: ip, Value: val})
	}
	return results, nil
}
