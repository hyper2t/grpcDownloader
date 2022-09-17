package server

import (
	fi "github.com/grpcBigFile/utils"
	"google.golang.org/grpc"
	"log"
	"net"
)

func Start() {
	// create listiner
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	service := &BytesTransferServer{}
	service.Register(s)
	service.SetBytesReceiver(&fi.Downloader{})
	service.SetBytesStreamer(&fi.FileStreamer{})

	log.Println("start server 192.168.162.100:8000")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
