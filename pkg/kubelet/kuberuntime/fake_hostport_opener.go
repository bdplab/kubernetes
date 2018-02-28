package kuberuntime

import (
	"fmt"
	"sync"
	"k8s.io/api/core/v1"
)

type hostport struct {
	port     int32
	protocol v1.Protocol
}

type fakeSocket struct {
	port     int32
	protocol v1.Protocol
	closed   bool
}

func (f *fakeSocket) Close() error {
	if f.closed {
		return fmt.Errorf("Socket %q.%s already closed!", f.port, f.protocol)
	}
	f.closed = true
	return nil
}

func NewFakeSocketManager() *fakeSocketManager {
	return &fakeSocketManager{mem: make(map[hostport]*fakeSocket)}
}

type fakeSocketManager struct {
	sync.RWMutex
	mem map[hostport]*fakeSocket
}

func (f *fakeSocketManager) openFakeSocket(port int32, protocol v1.Protocol) (closeable, error) {
	hp := hostport{port: port, protocol: protocol}
	f.Lock()
	defer f.Unlock()
	if socket, ok := f.mem[hp]; ok && !socket.closed {
		return nil, fmt.Errorf("hostport is occupied")
	}
	fs := &fakeSocket{hp.port, hp.protocol, false}
	f.mem[hp] = fs
	return fs, nil
}
