package utils

import (
	"fmt"
	"github.com/grpcBigFile/pb"
	"google.golang.org/protobuf/proto"
	"os"
)

type Downloader struct {
	f    *os.File
	path string
}

func (fr *Downloader) Init(msg proto.Message) error {
	info, ok := msg.(*pb.File)
	if !ok {
		return fmt.Errorf("failed to convert 'Message' type to 'File' type")
	}
	fr.path = info.Path

	var err error
	fr.f, err = os.Create(fr.path)
	if err != nil {
		return fmt.Errorf("failed to create '%s': %v", fr.path, err)
	}

	return nil
}

func (fr *Downloader) Push(data []byte, metadata proto.Message) error {
	_, err := fr.f.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to '%s' - file may be corrupted: %v", fr.path, err)
	}

	return nil
}

func (fr *Downloader) Finalize() error {
	if fr.f != nil {
		if err := fr.f.Close(); err != nil {
			return fmt.Errorf("failed to close '%s': %v", fr.path, err)
		}
	}

	return nil
}
