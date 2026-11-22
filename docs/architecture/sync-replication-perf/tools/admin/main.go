// admin connects the two perf clusters and registers global namespaces.
//
//	admin connect
//	admin ns <name>...
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	addrA = "127.0.0.1:7233"
	addrB = "127.0.0.1:8233"
)

func dial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	a, b := dial(addrA), dial(addrB)
	switch os.Args[1] {
	case "connect":
		// Each cluster reaches the other through its delay proxy.
		for _, c := range []struct {
			conn   *grpc.ClientConn
			remote string
		}{{a, "127.0.0.1:18233"}, {b, "127.0.0.1:17233"}} {
			_, err := operatorservice.NewOperatorServiceClient(c.conn).AddOrUpdateRemoteCluster(ctx, &operatorservice.AddOrUpdateRemoteClusterRequest{
				FrontendAddress:               c.remote,
				EnableRemoteClusterConnection: true,
				EnableReplication:             true,
			})
			if err != nil {
				log.Fatalf("connect %s: %v", c.remote, err)
			}
		}
		fmt.Println("clusters connected")
	case "ns":
		for _, name := range os.Args[2:] {
			_, err := workflowservice.NewWorkflowServiceClient(a).RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
				Namespace:                        name,
				IsGlobalNamespace:                true,
				ActiveClusterName:                "cluster-a",
				Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: "cluster-a"}, {ClusterName: "cluster-b"}},
				WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
			})
			if err != nil {
				log.Fatalf("register %s: %v", name, err)
			}
			for {
				_, err := workflowservice.NewWorkflowServiceClient(b).DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: name})
				if err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			fmt.Println("namespace", name, "registered and replicated to cluster-b")
		}
	}
}
