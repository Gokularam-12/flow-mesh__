package topology

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// PodInfo holds zone information for a pod.
type PodInfo struct {
	PodIP   string
	PodName string
	Service string
	Zone    string
}

// Reader reads pod topology from a static config file.
// In production this would talk to the Kubernetes API.
type Reader struct {
	pods []PodInfo
}

// NewReader creates a topology reader.
func NewReader() (*Reader, error) {
	return &Reader{}, nil
}

// LoadFromFile loads pod topology from a YAML file.
func (r *Reader) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &r.pods)
}

// LoadFromDir loads all topology YAML files from a directory.
func (r *Reader) LoadFromDir(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := r.LoadFromFile(f); err != nil {
			log.Printf("[topology] error loading %s: %v", f, err)
		}
	}
	return nil
}

// GetZone returns the zone for a given pod IP.
func (r *Reader) GetZone(podIP string) string {
	for _, p := range r.pods {
		if p.PodIP == podIP {
			return p.Zone
		}
	}
	return ""
}

// GetPodsInZone returns all pod IPs for a service in a given zone.
func (r *Reader) GetPodsInZone(service, zone string) []string {
	var pods []string
	for _, p := range r.pods {
		if p.Service == service && p.Zone == zone {
			pods = append(pods, p.PodIP)
		}
	}
	return pods
}

// SameZoneFirst reorders upstreams so same-zone pods come first
// and get higher weight. Falls back to all pods if no same-zone pods.
func (r *Reader) SameZoneFirst(service, callerZone string, upstreams []UpstreamWeight) []UpstreamWeight {
	if callerZone == "" {
		return upstreams
	}

	sameZone := []UpstreamWeight{}
	otherZone := []UpstreamWeight{}

	for _, u := range upstreams {
		zone := r.GetZone(u.PodIP)
		if zone == callerZone {
			sameZone = append(sameZone, u)
		} else {
			otherZone = append(otherZone, u)
		}
	}

	// If no same-zone pods, use all
	if len(sameZone) == 0 {
		return upstreams
	}

	// Give same-zone pods 80% of traffic, cross-zone 20%
	result := []UpstreamWeight{}
	sameZoneWeight := uint32(80 / len(sameZone))
	for _, u := range sameZone {
		u.Weight = sameZoneWeight
		result = append(result, u)
	}

	if len(otherZone) > 0 {
		crossZoneWeight := uint32(20 / len(otherZone))
		for _, u := range otherZone {
			u.Weight = crossZoneWeight
			result = append(result, u)
		}
	}

	return result
}

// UpstreamWeight is a simple pod IP + weight pair.
type UpstreamWeight struct {
	PodIP  string
	Weight uint32
	Zone   string
}

// GetCallerZone reads the caller zone from an environment variable.
// In production this comes from the node label via downward API.
func GetCallerZone() string {
	zone := os.Getenv("POD_ZONE")
	if zone == "" {
		zone = "us-east-1a" // default for local dev
	}
	return zone
}

// ZoneContext attaches zone info to a context.
func ZoneContext(ctx context.Context, zone string) context.Context {
	return context.WithValue(ctx, zoneKey{}, zone)
}

type zoneKey struct{}
