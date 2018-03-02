/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kuberuntime

import (
	"fmt"
	"net"
	"sync"

	"container/list"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"hash/fnv"
)

type hostportOpener func(int32, v1.Protocol) (closeable, error)

type closeable interface {
	Close() error
}

func openLocalPort(port int32, proto v1.Protocol) (closeable, error) {
	// NOTE: We should not need to have a real listen()ing socket - bind()
	// should be enough, but I can't figure out a way to e2e test without
	// it.  Tools like 'ss' and 'netstat' do not show sockets that are
	// bind()ed but not listen()ed, and at least the default debian netcat
	// has no way to avoid about 10 seconds of retries.
	switch proto {
	case v1.ProtocolTCP:
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, err
		}
		glog.V(3).Infof("Opened local port %v for TCP.", port)
		return listener, nil
	case v1.ProtocolUDP:
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return nil, err
		}
		glog.V(3).Infof("Opened local port %v for UDP.", port)
		return conn, nil
	default:
		return nil, fmt.Errorf("unknown protocol %v", proto)
	}
}

// Represents and manages a range of hosts ports that can be allocated.
type lruPortEntry struct {
	keyHash uint32
	port int32
}

type cachedHostPortRange struct {
	sync.RWMutex
	// The lowest port in the range.
	lowerPort       int32
	// Indicates whether a port in the range has been allocated. It has to be consistent with lruEntries.
	// This field is used to make sure the same port is not released multiple times.
	ports_allocated []bool
	// Maintains a LRU list of free ports in the range. The port at the front is released earlier, compared to the
	// port at the back, thus the front port should be allocated at first. The list should be consistent with ports_allocated field.
	lruEntries      *list.List
	// Maintains a port allocation cache to provide port stickiness.
	cache           map[uint32]*list.Element
}

func (pr *cachedHostPortRange) inRange(port int32) bool {
	return port >= pr.lowerPort && port < pr.lowerPort + int32(len(pr.ports_allocated))
}

// Returns the index corresponding to the given |port|.
func (pr *cachedHostPortRange) getPortIndex(port int32) int {
	if pr.inRange(port) {
		return int(port - pr.lowerPort)
	}
	return -1
}

func getCacheKeyHash(cacheKey string) uint32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(cacheKey))
	return hasher.Sum32()
}

// Requires lowerPort <= upperPort
func createCachedHostPortRange(lowerPort int32, upperPort int32) *cachedHostPortRange {
	if upperPort < lowerPort {
		upperPort = lowerPort
	}

	pr := &cachedHostPortRange{
		lowerPort: lowerPort,
		ports_allocated: make([]bool, upperPort - lowerPort),
	}
	pr.Initialize(nil)
	return pr
}

// Initialize the host port range given the already allocated ports.
func (pr *cachedHostPortRange) Initialize(allocatedPorts map[int32]bool) {
	pr.lruEntries = list.New()
	pr.cache = make(map[uint32]*list.Element)
	for index := len(pr.ports_allocated) - 1; index >= 0; index-- {
		// By default, treats all the ports have been allocated.
		pr.ports_allocated[index] = true
		port := int32(index) + pr.lowerPort
		// Release the port if it turns out not being allocated yet.
		if _, ok := allocatedPorts[port]; !ok {
			pr.releasePort(port, "")
		}
	}
}

// Test whether a given |port| has been allocated in the host.
func tryOpenPort(portOpener hostportOpener, port int32, proto v1.Protocol) bool {
	sock, err := portOpener(port, proto)
	if err == nil && sock != nil {
		// Note that when the socket is closed, in theory another process may be able to take the
		// allocated port at the worst case if the process binds to the port before the container
		// does so. This will result in that the container can not be started, and eventually restarted
		// by kubelet, which will allocate a new port for the container.
		sock.Close()
		return true
	}
	return false
}

func (pr *cachedHostPortRange) tryAllocatePortFromLRUEntry(portOpener hostportOpener, proto v1.Protocol, element *list.Element) int32 {
	if element == nil {
		return -1
	}
	if lruEntry, ok := element.Value.(*lruPortEntry); ok && pr.inRange(lruEntry.port) {
		port := lruEntry.port
		// Even if the port is already bound, it's still cleared from the cache, as the cached port is not valid.
		// Here it makes sure that only when the cache entry matches with the entry in lruEntries, the cache entry is deleted.
		if expectedElement, ok := pr.cache[lruEntry.keyHash]; ok && expectedElement == element {
			delete(pr.cache, lruEntry.keyHash)
			lruEntry.keyHash = 0
		}
		if tryOpenPort(portOpener, port, proto) {
			// Needs to keep ports_allocated to be consistent with lruEntries.
			if index := pr.getPortIndex(port); index >= 0 {
				if pr.ports_allocated[index] {
					glog.Errorf("Internal state mismatched for port %v", port)
					return -1
				}
				pr.ports_allocated[index] = true
			}
			pr.lruEntries.Remove(element)
			return port
		}
	}
	return -1
}

// Returns a free host port within the predefined range and not bound yet. If no such free port is available, returns -1.
// cacheKey is used to provide port allocation stickiness, i.e. a specific container is more likely to be allocated with
// the port it just released.
func (pr *cachedHostPortRange) getFreePort(portOpener hostportOpener, proto v1.Protocol, cacheKey string) int32 {
	// We need to hold the mutex when searching for a free port in the pre-allocated range, to avoid the race condition
	// when multiple pods try to allocate the port at the same time.
	pr.Lock()
	defer pr.Unlock()

	// Try to get the port from the cache first.
	if len(cacheKey) > 0 {
		cacheKeyHash := getCacheKeyHash(cacheKey)
		if element, ok := pr.cache[cacheKeyHash]; ok && element != nil {
			if port := pr.tryAllocatePortFromLRUEntry(portOpener, proto, element); port > 0 {
				return port
			}
		}
	}
	if pr.lruEntries.Len() <= 0 {
		return -1
	}
	// Tries to get the port from the LRU entry list, from the front (least recently released) to the back.
	for e := pr.lruEntries.Front(); e != nil; e = e.Next() {
		if port := pr.tryAllocatePortFromLRUEntry(portOpener, proto, e); port > 0 {
			// It's safe to remove e directly here as it returns immediately.
			return port
		}
	}
	// No available port
	return -1
}

// Indicates that the given |port| is released. |cacheKey| provides the stickiness to the next allocation: the same container
// is more likely to get this port at the next allocation.
func (pr *cachedHostPortRange) releasePort(port int32, cacheKey string) {
	index := pr.getPortIndex(port)
	if index < 0 {
		return
	}
	pr.Lock()
	defer pr.Unlock()

	if !pr.ports_allocated[index] {
		glog.Warningf("Try to release an unallocated port: %v for %v", port, cacheKey)
		return
	}
	// Need to ensure ports_allocated is consistent with lruEntries.
	pr.ports_allocated[index] = false
	if len(cacheKey) <= 0 {
		// Bypass the cache
		pr.lruEntries.PushFront(&lruPortEntry{port: port, keyHash: 0})
		return
	}
	cacheKeyHash := getCacheKeyHash(cacheKey)
	if element := pr.lruEntries.PushBack(&lruPortEntry{port: port, keyHash: cacheKeyHash}); element != nil {
		pr.cache[cacheKeyHash] = element
	}
}

// This method is for testing purpose only.
func (pr *cachedHostPortRange) isAllocated(port int32) bool {
	index := pr.getPortIndex(port)
	if index < 0 {
		return false
	}
	pr.Lock()
	defer pr.Unlock()
	return pr.ports_allocated[index]
}
