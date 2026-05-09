package xds

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoveryservice "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Upstream struct {
	PodIP   string
	Port    uint32
	Weight  uint32
	Zone    string
	PodName string
}

type RouteTable struct {
	Service   string
	Upstreams []Upstream
}

type Event struct {
	Time    time.Time
	Service string
	Message string
	Rule    string
}

type Server struct {
	cache  cachev3.SnapshotCache
	mu     sync.RWMutex
	routes map[string]*RouteTable
	events []Event
}

func NewServer(ctx context.Context) *Server {
	snapshotCache := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, nil)
	return &Server{
		cache:  snapshotCache,
		routes: make(map[string]*RouteTable),
	}
}

func (s *Server) Register(g *grpc.Server) {
	srv := serverv3.NewServer(context.Background(), s.cache, nil)
	discoveryservice.RegisterAggregatedDiscoveryServiceServer(g, srv)
	endpointservice.RegisterEndpointDiscoveryServiceServer(g, srv)
	clusterservice.RegisterClusterDiscoveryServiceServer(g, srv)
	routeservice.RegisterRouteDiscoveryServiceServer(g, srv)
	listenerservice.RegisterListenerDiscoveryServiceServer(g, srv)
}

func (s *Server) UpsertRoute(rt RouteTable) error {
	s.mu.Lock()
	s.routes[rt.Service] = &rt
	s.mu.Unlock()
	return s.pushSnapshot()
}

func (s *Server) SetWeight(service, podIP string, weight uint32) error {
	s.mu.Lock()
	rt, ok := s.routes[service]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("service %q not found", service)
	}
	for i, u := range rt.Upstreams {
		if u.PodIP == podIP {
			rt.Upstreams[i].Weight = weight
		}
	}
	s.mu.Unlock()
	return s.pushSnapshot()
}

func (s *Server) ShiftTrafficAway(service string, filter func(Upstream) bool, pct uint32) error {
	s.mu.Lock()
	rt, ok := s.routes[service]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("service %q not found", service)
	}
	for i, u := range rt.Upstreams {
		if filter(u) && rt.Upstreams[i].Weight >= pct {
			rt.Upstreams[i].Weight -= pct
		}
	}
	normalizeWeights(rt)
	s.mu.Unlock()
	return s.pushSnapshot()
}

func (s *Server) GetRoutes() map[string]RouteTable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]RouteTable, len(s.routes))
	for k, v := range s.routes {
		out[k] = *v
	}
	return out
}

func (s *Server) LogEvent(service, rule, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, Event{
		Time:    time.Now(),
		Service: service,
		Rule:    rule,
		Message: msg,
	})
	if len(s.events) > 200 {
		s.events = s.events[len(s.events)-200:]
	}
}

func (s *Server) GetEvents() []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

func (s *Server) pushSnapshot() error {
	s.mu.RLock()
	routes := make(map[string]*RouteTable, len(s.routes))
	for k, v := range s.routes {
		cp := *v
		routes[k] = &cp
	}
	s.mu.RUnlock()

	var clusters []types.Resource
	var endpoints []types.Resource

	for svc, rt := range routes {
		clusterName := "cluster_" + svc
		c := &cluster.Cluster{
			Name:                 clusterName,
			ConnectTimeout:       durationpb.New(5 * time.Second),
			ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
			EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
				EdsConfig: &core.ConfigSource{
					ConfigSourceSpecifier: &core.ConfigSource_Ads{
						Ads: &core.AggregatedConfigSource{},
					},
				},
			},
			LbPolicy: cluster.Cluster_ROUND_ROBIN,
		}
		clusters = append(clusters, c)

		var lbEndpoints []*endpoint.LbEndpoint
		for _, u := range rt.Upstreams {
			ep := &endpoint.LbEndpoint{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  u.PodIP,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: u.Port,
									},
								},
							},
						},
					},
				},
				LoadBalancingWeight: wrapperspb.UInt32(u.Weight),
			}
			lbEndpoints = append(lbEndpoints, ep)
		}

		cla := &endpoint.ClusterLoadAssignment{
			ClusterName: clusterName,
			Endpoints: []*endpoint.LocalityLbEndpoints{
				{LbEndpoints: lbEndpoints},
			},
		}
		endpoints = append(endpoints, cla)
	}

	version := uuid.New().String()
	snapshot, err := cachev3.NewSnapshot(version,
		map[resourcev3.Type][]types.Resource{
			resourcev3.ClusterType:  clusters,
			resourcev3.EndpointType: endpoints,
		},
	)
	if err != nil {
		return fmt.Errorf("snapshot validation failed: %w", err)
	}

	if err := s.cache.SetSnapshot(context.Background(), "flowmesh-node", snapshot); err != nil {
		return fmt.Errorf("set snapshot: %w", err)
	}

	log.Printf("[xds] pushed snapshot v=%s services=%d", version, len(routes))
	return nil
}

func normalizeWeights(rt *RouteTable) {
	total := uint32(0)
	for _, u := range rt.Upstreams {
		total += u.Weight
	}
	if total == 0 || total == 100 {
		return
	}
	for i := range rt.Upstreams {
		rt.Upstreams[i].Weight = rt.Upstreams[i].Weight * 100 / total
	}
}
