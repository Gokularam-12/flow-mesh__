package policy

import (
	"log"

	"github.com/flowmesh/flowmesh/internal/topology"
	"github.com/flowmesh/flowmesh/internal/xds"
)

// ApplyTopologyWeights adjusts upstream weights to prefer same-zone pods.
func ApplyTopologyWeights(
	xdsServer *xds.Server,
	topoReader *topology.Reader,
	service string,
	callerZone string,
) {
	routes := xdsServer.GetRoutes()
	rt, ok := routes[service]
	if !ok {
		return
	}

	upstreams := make([]topology.UpstreamWeight, len(rt.Upstreams))
	for i, u := range rt.Upstreams {
		upstreams[i] = topology.UpstreamWeight{
			PodIP:  u.PodIP,
			Weight: u.Weight,
			Zone:   u.Zone,
		}
	}

	adjusted := topoReader.SameZoneFirst(service, callerZone, upstreams)

	newUpstreams := make([]xds.Upstream, len(adjusted))
	for i, a := range adjusted {
		newUpstreams[i] = xds.Upstream{
			PodIP:  a.PodIP,
			Weight: a.Weight,
			Zone:   a.Zone,
			Port:   8080,
		}
	}

	err := xdsServer.UpsertRoute(xds.RouteTable{
		Service:   service,
		Upstreams: newUpstreams,
	})
	if err != nil {
		log.Printf("[topology] failed to update weights for %s: %v", service, err)
		return
	}

	log.Printf("[topology] applied zone-aware weights for service=%s callerZone=%s", service, callerZone)
	for _, a := range adjusted {
		log.Printf("[topology]   pod=%s zone=%s weight=%d", a.PodIP, a.Zone, a.Weight)
	}
}
