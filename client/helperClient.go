package client

import (
	"context"
	"fmt"
	"github.com/grpcBigFile/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"log"
)

// BytesReceiver represents an interface for bytes reception.
type BytesReceiver interface {

	// Init initialize the receiver.
	Init(msg proto.Message) error

	// Push processing the received bytes and their optional additional info.
	Push(data []byte, metadata proto.Message) error

	// Finalize clear resources of the receiver.
	Finalize() error
}

// CreateTransferClient returns gRPC client given a connection.
func CreateTransferClient(conn *grpc.ClientConn) pb.DownloadServiceClient {
	return pb.NewDownloadServiceClient(conn)
}

// Receive downloads bytes from destination to source.

func Download(client pb.DownloadServiceClient, ctx context.Context, fileStreamerMsg, downloaderMsg proto.Message, downloader BytesReceiver) (errout error) {

	any, err := anypb.New(fileStreamerMsg)
	if err != nil {
		return fmt.Errorf("failed to create 'Any' from streamer message: %v", err)
	}

	req := &pb.Info{
		Msg: any,
	}

	stream, err := client.Download(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch stream: %v", err)
	}

	if downloader == nil {
		return fmt.Errorf("receiver is nil")
	}

	if err := downloader.Init(downloaderMsg); err != nil {
		return fmt.Errorf("failed to init receiver: %v", err)
	}

	defer func() {
		if err := downloader.Finalize(); err != nil {
			if errout == nil {
				errout = fmt.Errorf("failed to finalize receiver: %v", err)
			}
		}
	}()

	errch := make(chan error)

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				errch <- nil
				log.Println("client recv stream finished.")
				return
			}
			if err != nil {
				errch <- fmt.Errorf("failed to receive: %v", err)
				return
			}

			data := res.Chunk.Data
			size := len(res.Chunk.Data)

			metadata, err := res.Info.Msg.UnmarshalNew()
			if err != nil {
				errch <- fmt.Errorf("failed to unmarshal 'Info.Msg': %v", err)
				return
			}

			if err := downloader.Push(data[:size], metadata); err != nil {
				errch <- fmt.Errorf("failed to push data to receiver: %v", err)
				return
			}
		}
	}()

	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
