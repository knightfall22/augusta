package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/knightfall22/augusta/internal/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	addr := fmt.Sprintf(":%d", 50051)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		log.Printf("[ERROR] failed to dial: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)

	stream, err := client.ConnectSession(context.Background())
	if err != nil {
		log.Printf("[ERROR] failed to instantiate stream: %v", err)
		return
	}

	// 1. Start a goroutine to continuously listen to the server
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// Server closed the stream cleanly
				close(waitc)
				return
			}
			if err != nil {
				log.Printf("[ERROR] Failed to receive a note: %v", err)
				close(waitc)
				return
			}
			log.Printf("[INFO] Got message from server: %T", in.Payload)
		}
	}()

	// 2. Send the registration message just once
	if err := stream.Send(&pb.ClientMessage{
		Payload: &pb.ClientMessage_Register{
			Register: &pb.RegisterWorker{
				WorkerId: "worker1",
			},
		},
	}); err != nil {
		log.Printf("[ERROR] failed to send: %v", err)
	}

	// 3. Keep the client alive to receive tasks or send periodic heartbeats
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if err := stream.Send(&pb.ClientMessage{
				Payload: &pb.ClientMessage_Heartbeat{
					Heartbeat: &pb.TaskHeartbeat{
						WorkerId: "worker1",
					},
				},
			}); err != nil {
				log.Printf("[ERROR] Heartbeat failed: %v", err)
				return
			}
		}
	}()

	// Wait for the stream to close
	<-waitc
	fmt.Println("EOF")
}
