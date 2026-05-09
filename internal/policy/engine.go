package policy

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/flowmesh/flowmesh/internal/metrics"
	"github.com/flowmesh/flowmesh/internal/xds"
	"gopkg.in/yaml.v3"
)

type RoutingPolicy struct {
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec PolicySpec `yaml:"spec"`
}

type PolicySpec struct {
	Service string       `yaml:"service"`
	Rules   []PolicyRule `yaml:"rules"`
}

type PolicyRule struct {
	If       string `yaml:"if"`
	Action   string `yaml:"action"`
	AwayFrom string `yaml:"away_from"`
	Target   string `yaml:"target"`
	By       string `yaml:"by"`
	Topology string `yaml:"topology"`
	LatencyThresholdMs float64
	ErrorRateThreshold float64
	CPUThreshold       float64
	ShiftPercent       uint32
}

type Engine struct {
	xds      *xds.Server
	scraper  *metrics.Scraper
	policies []RoutingPolicy
}

func NewEngine(x *xds.Server, s *metrics.Scraper) *Engine {
	return &Engine{xds: x, scraper: s}
}

func (e *Engine) LoadDir(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return err
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var p RoutingPolicy
		if err := yaml.Unmarshal(data, &p); err != nil {
			return fmt.Errorf("parse %s: %w", f, err)
		}
		for i := range p.Spec.Rules {
			parseRule(&p.Spec.Rules[i])
		}
		e.policies = append(e.policies, p)
		log.Printf("[policy] loaded %s → service=%s rules=%d", f, p.Spec.Service, len(p.Spec.Rules))
	}
	return nil
}

func (e *Engine) Evaluate(ctx context.Context) {
	signals, err := e.scraper.FetchSignals(ctx)
	if err != nil {
		log.Printf("[policy] metrics fetch error: %v", err)
		return
	}
	byIP := map[string]metrics.Signals{}
	for _, s := range signals {
		byIP[s.PodIP] = s
	}
	for _, p := range e.policies {
		e.evaluatePolicy(p, byIP)
	}
}

func (e *Engine) evaluatePolicy(p RoutingPolicy, byIP map[string]metrics.Signals) {
	routes := e.xds.GetRoutes()
	rt, ok := routes[p.Spec.Service]
	if !ok {
		return
	}
	for _, rule := range p.Spec.Rules {
		switch rule.Action {
		case "circuit_break":
			for _, u := range rt.Upstreams {
				sig, ok := byIP[u.PodIP]
				if !ok {
					continue
				}
				if rule.ErrorRateThreshold > 0 && sig.ErrorRate > rule.ErrorRateThreshold {
					log.Printf("[policy] CIRCUIT BREAK service=%s pod=%s errRate=%.4f", p.Spec.Service, u.PodIP, sig.ErrorRate)
					_ = e.xds.SetWeight(p.Spec.Service, u.PodIP, 0)
					e.xds.LogEvent(p.Spec.Service, rule.If, fmt.Sprintf("circuit break pod=%s errRate=%.2f%%", u.PodIP, sig.ErrorRate*100))
				}
			}
		case "shift_traffic":
			for _, u := range rt.Upstreams {
				sig, ok := byIP[u.PodIP]
				if !ok {
					continue
				}
				if rule.LatencyThresholdMs > 0 && sig.LatencyMs > rule.LatencyThresholdMs {
					pct := rule.ShiftPercent
					if pct == 0 {
						pct = 20
					}
					log.Printf("[policy] SHIFT TRAFFIC service=%s pod=%s latency=%.2fms", p.Spec.Service, u.PodIP, sig.LatencyMs)
					_ = e.xds.ShiftTrafficAway(p.Spec.Service, func(up xds.Upstream) bool { return up.PodIP == u.PodIP }, pct)
					e.xds.LogEvent(p.Spec.Service, rule.If, fmt.Sprintf("shifted %d%% away from pod=%s latency=%.2fms", pct, u.PodIP, sig.LatencyMs))
				}
			}
		case "reduce_weight":
			for _, u := range rt.Upstreams {
				sig, ok := byIP[u.PodIP]
				if !ok {
					continue
				}
				if rule.CPUThreshold > 0 && sig.CPUPercent > rule.CPUThreshold {
					pct := rule.ShiftPercent
					if pct == 0 {
						pct = 30
					}
					log.Printf("[policy] REDUCE WEIGHT service=%s pod=%s cpu=%.2f%%", p.Spec.Service, u.PodIP, sig.CPUPercent)
					_ = e.xds.ShiftTrafficAway(p.Spec.Service, func(up xds.Upstream) bool { return up.PodIP == u.PodIP }, pct)
					e.xds.LogEvent(p.Spec.Service, rule.If, fmt.Sprintf("reduced weight %d%% pod=%s cpu=%.2f%%", pct, u.PodIP, sig.CPUPercent))
				}
			}
		}
	}
}

func parseRule(r *PolicyRule) {
	s := strings.TrimSpace(r.If)
	if strings.HasPrefix(s, "latency_p99") {
		var val float64
		fmt.Sscanf(extractValue(s), "%f", &val)
		r.LatencyThresholdMs = val
	} else if strings.HasPrefix(s, "error_rate") {
		var val float64
		fmt.Sscanf(extractValue(s), "%f", &val)
		r.ErrorRateThreshold = val / 100
	} else if strings.HasPrefix(s, "cpu") {
		var val float64
		fmt.Sscanf(extractValue(s), "%f", &val)
		r.CPUThreshold = val
	}
	by := strings.TrimSuffix(strings.TrimSpace(r.By), "%")
	var pct uint32
	fmt.Sscanf(by, "%d", &pct)
	r.ShiftPercent = pct
}

func extractValue(s string) string {
	idx := strings.Index(s, "> ")
	if idx < 0 {
		return "0"
	}
	val := strings.TrimSpace(s[idx+2:])
	val = strings.TrimSuffix(val, "ms")
	val = strings.TrimSuffix(val, "%")
	return val
}
