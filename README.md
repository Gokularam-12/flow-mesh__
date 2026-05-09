# FlowMesh 🔀

> Policy-Driven Adaptive Service Routing Engine — built on the same layer as Istio, Consul, and AWS App Mesh.

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat-square&logo=go)
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.35-326CE5?style=flat-square&logo=kubernetes)
![Envoy](https://img.shields.io/badge/Envoy-xDS_v3-AC6EFF?style=flat-square)
![Prometheus](https://img.shields.io/badge/Prometheus-scraping-E6522C?style=flat-square&logo=prometheus)
![React](https://img.shields.io/badge/Dashboard-React-61DAFB?style=flat-square&logo=react)

---

## What Is FlowMesh?

FlowMesh is a **custom xDS control plane** that controls how traffic moves between microservices in real time — based on live signals like latency, error rate, and CPU usage.

Most engineers use Istio or Linkerd as a black box. FlowMesh is built at the layer those tools are built on — speaking directly to the **Envoy xDS v3 ADS gRPC protocol**, pushing routing snapshots to live Envoy sidecars with zero restarts and zero redeployments.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                       │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           FlowMesh Control Plane (Go)                │   │
│  │                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │   │
│  │  │ xDS Server   │  │Policy Engine │  │ REST API  │ │   │
│  │  │ gRPC :18000  │  │  (10s tick)  │  │  :8081    │ │   │
│  │  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘ │   │
│  │         │                 │                 │        │   │
│  │         │          ┌──────▼───────┐         │        │   │
│  │         │          │  Prometheus  │         │        │   │
│  │         │          │  Scraper     │         │        │   │
│  │         │          └──────────────┘         │        │   │
│  └─────────┼───────────────────────────────────┼────────┘   │
│            │ xDS snapshot (gRPC stream)         │            │
│            ▼                                   ▼            │
│  ┌─────────────────┐  ┌─────────────────┐   Dashboard      │
│  │  user-service   │  │  order-service  │   React :3000    │
│  │  ┌───────────┐  │  │  ┌───────────┐  │                  │
│  │  │   App     │  │  │  │   App     │  │                  │
│  │  ├───────────┤  │  │  ├───────────┤  │                  │
│  │  │  Envoy    │  │  │  │  Envoy    │  │                  │
│  │  │  sidecar  │  │  │  │  sidecar  │  │                  │
│  │  └───────────┘  │  │  └───────────┘  │                  │
│  └─────────────────┘  └─────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Features

### ⚡ Real-Time xDS Control Plane
- Implements Envoy **ADS (Aggregated Discovery Service)** gRPC protocol v3
- Pushes `ClusterLoadAssignment` snapshots to all Envoy instances instantly
- No restart. No redeploy. Weight changes propagate in milliseconds
- Built with the official `go-control-plane` library from the Envoy project

### 📋 Policy Engine
Define routing rules as YAML — they become live routing decisions every 10 seconds:

```yaml
apiVersion: flowmesh.io/v1
kind: RoutingPolicy
metadata:
  name: payment-policy
spec:
  service: payment-service
  rules:
    - if: latency_p99 > 200ms
      action: shift_traffic
      by: 20%
    - if: error_rate > 5%
      action: circuit_break
      target: pod
    - if: cpu > 80%
      action: reduce_weight
      by: 30%
```

### 🔴 Circuit Breaker
- Pod crosses error threshold → weight set to 0 instantly
- Envoy stops routing any traffic to that pod
- No human intervention required

### 📊 Prometheus Integration
Queries live metrics every 10 seconds via PromQL:
```promql
rate(envoy_upstream_rq_time_sum[1m]) / rate(envoy_upstream_rq_time_count[1m])
rate(envoy_upstream_rq_xx{response_code_class="5"}[1m])
rate(container_cpu_usage_seconds_total[1m]) * 100
```

### 🗺️ Topology-Aware Routing
- Reads pod zone labels (`us-east-1a`, `us-east-1b`)
- Routes **80% traffic to same-zone pods**, 20% cross-zone fallback
- Reduces cross-AZ latency and cloud egress costs ($0.01/GB at AWS)

### 📺 Live React Dashboard
- Service flow graph with health indicators
- Per-pod traffic weight bars with live updates
- Circuit breaker status (green / orange / red)
- Policy event feed — every decision logged with reason and timestamp
- Polls control plane REST API every 5 seconds

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cluster | Kubernetes (minikube) |
| Service Proxy | Envoy v1.29 |
| xDS Protocol | ADS gRPC v3 |
| Control Plane | Go 1.23 + go-control-plane |
| Metrics | Prometheus + PromQL |
| Policies | YAML CRDs |
| Dashboard | React + TypeScript + Recharts |
| Automation | Bash |

---

## Project Structure

```
flowmesh/
├── cmd/
│   └── flowmesh/
│       └── main.go              # Entrypoint — wires everything together
├── internal/
│   ├── xds/
│   │   └── server.go            # xDS control plane — pushes snapshots to Envoy
│   ├── metrics/
│   │   └── scraper.go           # Prometheus scraper — fetches live signals
│   ├── policy/
│   │   ├── engine.go            # Policy evaluator — runs rules every 10s
│   │   └── topology.go          # Topology-aware weight adjuster
│   ├── topology/
│   │   └── reader.go            # Zone reader — maps pods to availability zones
│   └── api/
│       └── handler.go           # REST API — serves routes + events to dashboard
├── k8s/
│   └── manifests/
│       ├── services.yaml        # user/order/payment service deployments
│       ├── envoy-bootstrap.yaml # Envoy xDS bootstrap configmap
│       ├── prometheus.yaml      # Prometheus deployment + RBAC
│       ├── control-plane.yaml   # FlowMesh control plane deployment
│       └── patch-envoy-sidecar.yaml  # Envoy sidecar injection patch
├── policies/
│   ├── payment-policy.yaml      # Payment service routing rules
│   ├── user-policy.yaml         # User service routing rules
│   └── topology.yaml            # Pod zone assignments
├── Dockerfile                   # Multi-stage build
├── start-flowmesh.sh            # One-command startup script
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker
- minikube
- kubectl
- Go 1.23+
- Node.js 20+

### 1. Clone the repo
```bash
git clone https://github.com/Gokularam-12/flow-mesh__.git
cd flow-mesh__
```

### 2. Start everything
```bash
chmod +x start-flowmesh.sh
./start-flowmesh.sh
```

This starts minikube, deploys all services, injects Envoy sidecars, deploys Prometheus and the control plane, and sets up port forwarding.

### 3. Start the dashboard
```bash
cd flowmesh-dashboard
npm install
npm start
```

Open **http://localhost:3000**

### 4. View control plane logs
```bash
kubectl logs -f deployment/flowmesh-control-plane
```

---

## How It Works — The Key Insight

When you update a routing weight in FlowMesh:

1. `policy engine` evaluates a rule → calls `xdsServer.SetWeight()`
2. `xdsServer` updates the in-memory `RouteTable`
3. `pushSnapshot()` builds a new `ClusterLoadAssignment` protobuf
4. The snapshot is pushed to **all connected Envoy instances** via the open gRPC stream
5. Envoy applies the new weights **instantly** — no pod restart, no config reload

This is the same mechanism that Istio uses internally when you apply a `VirtualService`. FlowMesh exposes it directly.

---

## Routing Table

```go
type RouteTable struct {
    Service   string
    Upstreams []Upstream
}

type Upstream struct {
    PodIP   string
    Port    uint32
    Weight  uint32  // 0 = circuit broken, 1-100 = % of traffic
    Zone    string  // availability zone for topology routing
    PodName string
}
```

---

## REST API

| Endpoint | Description |
|---|---|
| `GET /api/routes` | Current routing table for all services |
| `GET /api/events` | Policy event log (last 200 events) |

---

## Why This Is Different

| What most engineers do | What FlowMesh does |
|---|---|
| `kubectl apply` an Istio VirtualService | Implements the xDS protocol Istio is built on |
| Uses Helm charts for service mesh | Builds the control plane from scratch |
| Treats circuit breaking as a black box | Implements circuit breaking at the routing layer |
| Unaware of cross-AZ costs | Builds topology-aware routing to minimize them |

---

## Phases Built

- ✅ **Phase 1** — Kubernetes cluster + Envoy sidecars (2/2 per pod)
- ✅ **Phase 2** — Go xDS control plane (gRPC ADS stream)
- ✅ **Phase 3** — Prometheus scraping + PromQL signal collection
- ✅ **Phase 4** — YAML policy engine (circuit break, shift traffic, reduce weight)
- ✅ **Phase 5** — Topology-aware routing (same-zone preference 80/20)
- ✅ **Phase 6** — React live dashboard with service graph + event feed

---

## Author

**Gokularam** — Built from scratch, every layer, end to end.

> *"95% of engineers who use service meshes have never seen an xDS message. This project lives at the layer most never get past."*

---

## License

MIT
