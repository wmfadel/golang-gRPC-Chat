package main

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	"gRPC-Chat/proto"

	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

// Connection struct
type Connection struct {
	stream proto.Broadcast_CreateStreamZServer
	id     string
	active bool
	error  chan error
}

// Server struct
type Server struct {
	Connection []*Connection
}

// CreateStreamZ func
func (s *Server) CreateStreamZ(pconn *proto.Connect, pstream proto.Broadcast_CreateStreamZServer) error {
	conn := &Connection{
		stream: pstream,
		id:     pconn.User.Id,
		active: true,
		error:  make(chan error),
	}

	s.Connection = append(s.Connection, conn)
	return <-conn.error
}

// BroadCastMessageZ func
func (s *Server) BroadCastMessageZ(ctx context.Context, m *proto.Message) (*proto.Close, error) {
	w := sync.WaitGroup{}
	done := make(chan int)
	for _, conn := range s.Connection {
		w.Add(1)
		go func(msg *proto.Message, con *Connection) {
			defer w.Done()
			if con.active {
				err := con.stream.Send(m)
				grpcLog.Info("Sending message to: ", conn.stream)

				if err != nil {
					grpcLog.Errorf("Error with Stream: %v - Error: %v", conn.stream, err)
					conn.active = false
					conn.error <- err
				}
			}
		}(m, conn)
	}
	go func() {
		w.Wait()
		close(done)
	}()

	<-done
	return &proto.Close{}, nil
}

func main() {
	var connections []*Connection

	server := &Server{connections}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating the server %v", err)
	}

	grpcLog.Info("Starting server at port :8080")

	proto.RegisterBroadcastServer(grpcServer, server)
	grpcServer.Serve(listener)
}

//--proto_path=proto --proto_path=third_party --go_out=plugins=grpc:proto service.proto
