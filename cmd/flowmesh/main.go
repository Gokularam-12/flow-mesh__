package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flowmesh/flowmesh/internal/api"
	"github.com/flowmesh/flowmesh/internal/metrics"
	"github.com/flowmesh/flowmesh/internal/policy"
	"github.com/flowmesh/flowmesh/internal/topology"
	"github.com/flowmesh/flowmesh/internal/xds"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Println("[flowmesh] starting control plane")

	xdsServer := xds.NewServer(ctx)
	grpcSrv := grpc.NewServer()
	xdsServer.Register(grpcSrv)

	grpcLis, err := net.Listen("tcp", ":18000")
	if err != nil {
		log.Fatalf("failed to listen on :18000: %v", err)
	}
	go func() {
		log.Println("[flowmesh] xDS gRPC listening on :18000")
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Printf("[flowmesh] gRPC server error: %v", err)
		}
	}()

	_ = xdsServer.UpsertRoute(xds.RouteTable{
		Service: "user-service",
		Upstreams: []xds.Upstream{
			{PodIP: "10.244.0.24", Port: 8080, Weight: 50, Zone: "us-east-1a"},
			{PodIP: "10.244.0.28", Port: 8080, Weight: 50, Zone: "us-east-1b"},
		},
	})
	_ = xdsServer.UpsertRoute(xds.RouteTable{
		Service: "payment-service",
		Upstreams: []xds.Upstream{
			{PodIP: "10.244.0.26", Port: 8080, Weight: 50, Zone: "us-east-1a"},
			{PodIP: "10.244.0.30", Port: 8080, Weight: 50, Zone: "us-east-1b"},
		},
	})
	_ = xdsServer.UpsertRoute(xds.RouteTable{
		Service: "order-service",
		Upstreams: []xds.Upstream{
			{PodIP: "10.244.0.25", Port: 8080, Weight: 50, Zone: "us-east-1a"},
			{PodIP: "10.244.0.29", Port: 8080, Weight: 50, Zone: "us-east-1b"},
		},
	})

	// Topology reader
	topoReader, _ := topology.NewReader()
	topoReader.LoadFromDir(envOr("POLICY_DIR", "/policies"))

	// Apply topology-aware weights on startup
	callerZone := topology.GetCallerZone()
	log.Printf("[topology] caller zone: %s", callerZone)
	for _, svc := range []string{"user-service", "order-service", "payment-service"} {
		policy.ApplyTopologyWeights(xdsServer, topoReader, svc, callerZone)
	}

	promAddr := envOr("PROMETHEUS_ADDR", "http://prometheus:9090")
	scraper := metrics.NewScraper(promAddr)

	policyEngine := policy.NewEngine(xdsServer, scraper)
	policyDir := envOr("POLICY_DIR", "/policies")
	if err := policyEngine.LoadDir(policyDir); err != nil {
		log.Printf("[policy] load error: %v", err)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				policyEngine.Evaluate(ctx)
				// Re-apply topology weights every 30s
			case <-ctx.Done():
				return
			}
		}
	}()

	restHandler := api.NewHandler(xdsServer)
	httpSrv := &http.Server{
		Addr:    ":8081",
		Handler: restHandler,
	}
	go func() {
		log.Println("[flowmesh] REST API listening on :8081")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[flowmesh] HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[flowmesh] shutting down...")
	grpcSrv.GracefulStop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
