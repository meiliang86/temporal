// admin connects the perf clusters and registers global namespaces.
//
//	admin connect
//	admin ns <name>...
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Clusters: frontend address, and the delay-proxy address the other clusters use to reach it.
var clusters = []struct{ name, addr, proxy string }{
	{"cluster-a", "127.0.0.1:7233", "127.0.0.1:17233"},
	{"cluster-b", "127.0.0.1:8233", "127.0.0.1:18233"},
	{"cluster-c", "127.0.0.1:9233", "127.0.0.1:19233"},
}

func dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

// CLUSTERS (default "cluster-a,cluster-b") selects the clusters to connect and to register namespaces
// on. The first one is the active cluster.
func selected() []int {
	names := os.Getenv("CLUSTERS")
	if names == "" {
		names = "cluster-a,cluster-b"
	}
	var out []int
	for _, n := range strings.Split(names, ",") {
		for i, c := range clusters {
			if c.name == n {
				out = append(out, i)
			}
		}
	}
	return out
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sel := selected()
	switch os.Args[1] {
	case "connect":
		// Each cluster reaches every other one through that cluster's delay proxy.
		for _, i := range sel {
			conn := dial(clusters[i].addr)
			for _, j := range sel {
				if i == j {
					continue
				}
				_, err := operatorservice.NewOperatorServiceClient(conn).AddOrUpdateRemoteCluster(ctx, &operatorservice.AddOrUpdateRemoteClusterRequest{
					FrontendAddress:               clusters[j].proxy,
					EnableRemoteClusterConnection: true,
					EnableReplication:             true,
				})
				if err != nil {
					log.Fatalf("connect %s -> %s: %v", clusters[i].name, clusters[j].name, err)
				}
			}
		}
		fmt.Println("clusters connected")
	case "point":
		// point <cluster> <address>: make <cluster> reach the remote cluster served at <address> there.
		for _, c := range clusters {
			if c.name != os.Args[2] {
				continue
			}
			_, err := operatorservice.NewOperatorServiceClient(dial(c.addr)).AddOrUpdateRemoteCluster(ctx, &operatorservice.AddOrUpdateRemoteClusterRequest{
				FrontendAddress:               os.Args[3],
				EnableRemoteClusterConnection: true,
				EnableReplication:             true,
			})
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(c.name, "now reaches", os.Args[3])
		}
	case "ns":
		var configs []*replicationpb.ClusterReplicationConfig
		for _, i := range sel {
			configs = append(configs, &replicationpb.ClusterReplicationConfig{ClusterName: clusters[i].name})
		}
		active := dial(clusters[sel[0]].addr)
		for _, name := range os.Args[2:] {
			_, err := workflowservice.NewWorkflowServiceClient(active).RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
				Namespace:                        name,
				IsGlobalNamespace:                true,
				ActiveClusterName:                clusters[sel[0]].name,
				Clusters:                         configs,
				WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
			})
			if err != nil {
				log.Fatalf("register %s: %v", name, err)
			}
			for _, i := range sel[1:] {
				conn := dial(clusters[i].addr)
				for {
					_, err := workflowservice.NewWorkflowServiceClient(conn).DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: name})
					if err == nil {
						break
					}
					time.Sleep(time.Second)
				}
			}
			fmt.Println("namespace", name, "registered and replicated to", os.Getenv("CLUSTERS"))
		}
	}
}
