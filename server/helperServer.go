package server

import (
	"context"
	"github.com/grpcBigFile/client"
	"github.com/grpcBigFile/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BytesStreamer represents an interface for bytes streaming.
type BytesStreamer interface {

	// Init initialize the streamer.
	Init(msg proto.Message) error

	// HasNext returns true if there are more bytes to stream.
	HasNext() bool

	// GetNext returns the next bytes with optional additional info.
	GetNext() ([]byte, proto.Message, error)

	// Finalize clear resources of the streamer.
	Finalize() error
}

// SetBytesStreamer registers a bytes streamer to the gRPC server.
func (s *BytesTransferServer) SetBytesStreamer(streamer BytesStreamer) {
	s.streamer = streamer
}

// SetBytesReceiver registers a bytes receiver to the gRPC server.
func (s *BytesTransferServer) SetBytesReceiver(receiver client.BytesReceiver) {
	s.receiver = receiver
}

// Register registers a bytes transferring service to a given gRPC server.
func (s *BytesTransferServer) Register(server *grpc.Server) {
	pb.RegisterDownloadServiceServer(server, s)
}

// BytesTransferServer represents an implementation of bytes transferring gRPC server.
type BytesTransferServer struct {
	pb.UnimplementedDownloadServiceServer

	streamer BytesStreamer
	receiver client.BytesReceiver
}

// Receive is the bytes download implementation of the bytes transferring service.
// comment: should not be used directly.

func (s *BytesTransferServer) Download(in *pb.Info, stream pb.DownloadService_DownloadServer) (errout error) {

	if stream.Context().Err() == context.Canceled {
		return status.Errorf(codes.Canceled, "client cancelled, abandoning")
	}

	if s.streamer == nil {
		return status.Errorf(codes.FailedPrecondition, "streamer is nil")
	}

	streamerMsg, err := in.Msg.UnmarshalNew()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to unmarshal 'Info.Msg': %v", err)
	}

	if err := s.streamer.Init(streamerMsg); err != nil {
		return status.Errorf(codes.FailedPrecondition, "failed to init streamer: %v", err)
	}
	defer func() {
		if err := s.streamer.Finalize(); err != nil {
			if errout == nil {
				errout = status.Errorf(codes.FailedPrecondition, "failed to finalize streamer: %v", err)
			}
		}
	}()

	for s.streamer.HasNext() {
		buf, metadata, err := s.streamer.GetNext()
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "failed to read chunk from streamer: %v", err)
		}

		any, err := anypb.New(metadata)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to create 'Any' from streamer metadata message: %v", err)
		}

		req := &pb.Packet{
			Info: &pb.Info{
				Msg: any,
			},
			Chunk: &pb.Chunk{
				Data: buf,
			},
		}

		if err := stream.Send(req); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
	}

	return nil
}
