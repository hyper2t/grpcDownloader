package client

import (
	"context"
	"github.com/grpcBigFile/pb"
	fi "github.com/grpcBigFile/utils"
	"google.golang.org/grpc"
	"log"
)

func Start() {
	// dail server
	conn, err := grpc.Dial("192.168.162.100:8000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("can not connect with server %v", err)
	}

	// create stream
	client := pb.NewDownloadServiceClient(conn)
	fromInfo := &pb.File{Path: "/root/Go.zip"}
	toInfo := &pb.File{Path: "/root/Go.zip"}
	if err := Download(client, context.Background(), fromInfo, toInfo, &fi.Downloader{}); err != nil {
		log.Fatalf("client failed: %v", err)
	}
}
