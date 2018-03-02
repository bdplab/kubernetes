package kuberuntime

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"math/rand"
	"fmt"
)

func TestGetFreePorts(t *testing.T) {
	opener := NewFakeSocketManager()
	var upperPort int32 = 1028
	var lowerPort int32 = 1025
	hostPortRange := createCachedHostPortRange(lowerPort, upperPort)
	testCases := []struct {
		protocol  v1.Protocol
		allocated bool
	}{
		{v1.ProtocolTCP, true},
		{v1.ProtocolTCP, true},
		{v1.ProtocolTCP, true},
		{v1.ProtocolTCP, false},
	}

	for _, tc := range testCases {
		port := hostPortRange.getFreePort(opener.openFakeSocket, tc.protocol, "")
		if tc.allocated {
			assert.True(t, port >= lowerPort && port < upperPort, "Allocated %d for %s", port, tc.protocol)
		} else {
			assert.True(t, port < 0)
		}
	}
}

func TestGetFreePortsInParallel(t *testing.T) {
	opener := NewFakeSocketManager()
	var upperPort int32 = 1200
	var lowerPort int32 = 1025
	hostPortRange := createCachedHostPortRange(lowerPort, upperPort)
	allocatedPorts := struct {
		sync.RWMutex
		ports map[int32]int
	}{
		ports: make(map[int32]int),
	}

	runTimes := 256
	wg := sync.WaitGroup{}
	wg.Add(runTimes)
	for i := 0; i < runTimes; i += 1 {
		go func() {
			defer wg.Done()
			if port := hostPortRange.getFreePort(opener.openFakeSocket, v1.ProtocolTCP, ""); port > 0 {
				assert.True(t, port >= lowerPort && port < upperPort)
				allocatedPorts.Lock()
				defer allocatedPorts.Unlock()
				allocatedPorts.ports[port] += 1
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int(upperPort - lowerPort), len(allocatedPorts.ports))
	for _, times := range allocatedPorts.ports {
		assert.Equal(t, 1, times)
	}
}

func TestInvalidPortRange(t *testing.T) {
	opener := NewFakeSocketManager().openFakeSocket
	hostPortRange := createCachedHostPortRange(1025, 1024)
	assert.True(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, "") < 0)
}

func TestHostPortNotPhysicallyReleased(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	cacheKey := ""
	hostPortRange := createCachedHostPortRange(lowerPort, 1025)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, cacheKey)
	assert.Equal(t, lowerPort, allocatedPort)
	opener(allocatedPort, v1.ProtocolTCP)
	// Although the port 1024 is released, it's still bound physically so cannot be allocated again.
	hostPortRange.releasePort(lowerPort, cacheKey)
	assert.True(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, cacheKey) < 0)
	// Now unbinds the physical port
	socketManager.mem[hostport{port: allocatedPort, protocol: v1.ProtocolTCP}].Close()
	// Then the port can be allocated again.
	assert.Equal(t, lowerPort, hostPortRange.getFreePort(opener, v1.ProtocolTCP, cacheKey))
}

func TestAllocateReleaseInParallel(t *testing.T) {
	t.Parallel()
	opener := NewFakeSocketManager().openFakeSocket
	var upperPort int32 = 1200
	var lowerPort int32 = 1025
	hostPortRange := createCachedHostPortRange(lowerPort, upperPort)

	runTimes := 256
	wg := sync.WaitGroup{}
	wg.Add(runTimes)
	for i := 0; i < runTimes; i += 1 {
		go func(index int) {
			cacheKey := fmt.Sprintf("container-%d", index)
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			var port int32
			for {
				if port = hostPortRange.getFreePort(opener, v1.ProtocolTCP, cacheKey); port > 0 {
					assert.True(t, port >= lowerPort && port < upperPort)
					break
				}
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			hostPortRange.releasePort(port, cacheKey)
		}(i)
	}
	wg.Wait()

	for port := lowerPort; port < upperPort; port += 1 {
		assert.False(t, hostPortRange.isAllocated(port), "Port %d was not released", port)
	}
}

func TestCachedHostPortNotPhysicallyReleased(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"
	hostPortRange := createCachedHostPortRange(lowerPort, 1025)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	opener(allocatedPort, v1.ProtocolTCP)
	// Although the port 1024 is released, it's still bound physically so cannot be allocated again.
	hostPortRange.releasePort(allocatedPort, portCacheKey)
	assert.True(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey) < 0)
	// The cached port should be released
	assert.Empty(t, hostPortRange.cache)
	assert.False(t, hostPortRange.isAllocated(lowerPort))
	// Now unbinds the physical port
	socketManager.mem[hostport{port: allocatedPort, protocol: v1.ProtocolTCP}].Close()
	// Then the port can be allocated again.
	assert.Equal(t, lowerPort, hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey))
	assert.True(t, hostPortRange.isAllocated(lowerPort))
}

func TestAllocatePortFromCache(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"
	portCacheKey2 := "cache2"

	hostPortRange := createCachedHostPortRange(lowerPort, 1026)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.True(t, allocatedPort >= lowerPort)
	// Release the port which goes to the cache.
	hostPortRange.releasePort(allocatedPort, portCacheKey)
	// Allocate a new port which has different cache key and is allocated directly.
	newAllocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2)
	assert.True(t, newAllocatedPort >= lowerPort)
	assert.NotEqual(t, allocatedPort, newAllocatedPort)
	// Now reallocate a port with the same cache key
	assert.False(t, hostPortRange.isAllocated(allocatedPort))
	reallocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.Equal(t, allocatedPort, reallocatedPort)
	assert.True(t, hostPortRange.isAllocated(allocatedPort))
}

func TestAllocatePortFromOtherCacheEntry(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"
	portCacheKey2 := "cache2"

	hostPortRange := createCachedHostPortRange(lowerPort, 1026)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.True(t, allocatedPort >= lowerPort)
	allocatedPort2 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2)
	assert.True(t, allocatedPort2 >= lowerPort)
	assert.NotEqual(t, allocatedPort, allocatedPort2)
	// Release the port which goes to the cache.
	hostPortRange.releasePort(allocatedPort, portCacheKey)
	// Allocate a new port which should be from the cache entry portCacheKey
	portCacheKey3 := "cache3"
	assert.False(t, hostPortRange.isAllocated(allocatedPort))
	allocatedPort3 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey3)
	assert.Equal(t, allocatedPort, allocatedPort3)
	assert.True(t, hostPortRange.isAllocated(allocatedPort))
}

func TestFreePortWithWrongCacheKey(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"
	portCacheKey2 := "cache2"

	hostPortRange := createCachedHostPortRange(lowerPort, 1026)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.True(t, allocatedPort >= lowerPort)
	allocatedPort2 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2)
	assert.True(t, allocatedPort2 >= lowerPort)
	assert.NotEqual(t, allocatedPort, allocatedPort2)
	// Release the port with the wrong cache entry.
	hostPortRange.releasePort(allocatedPort, portCacheKey2)
	// Release the port with the correct cache entry, which overrides the cache entry added above.
	hostPortRange.releasePort(allocatedPort2, portCacheKey2)
	assert.False(t, hostPortRange.isAllocated(allocatedPort))
	assert.False(t, hostPortRange.isAllocated(allocatedPort2))

	{
		// This should be allocated through the LRU list.
		allocatedPort3 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
		assert.Equal(t, allocatedPort, allocatedPort3)
		assert.True(t, hostPortRange.isAllocated(allocatedPort3))
	}
	{
		// This should be allocated through the cache.
		allocatedPort3 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2)
		assert.Equal(t, allocatedPort2, allocatedPort3)
		assert.True(t, hostPortRange.isAllocated(allocatedPort3))
	}
}

func TestPortFromOtherCacheEntryNotPhysicallyReleased(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"
	hostPortRange := createCachedHostPortRange(lowerPort, 1025)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	opener(allocatedPort, v1.ProtocolTCP)
	// Although the port 1024 is released, it's still bound physically so cannot be allocated again.
	hostPortRange.releasePort(allocatedPort, portCacheKey)
	portCacheKey2 := "cache2"
	assert.True(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2) < 0)
	// The cached port should be released
	assert.Empty(t, hostPortRange.cache)
	assert.False(t, hostPortRange.isAllocated(lowerPort))
	// Now unbinds the physical port
	socketManager.mem[hostport{port: allocatedPort, protocol: v1.ProtocolTCP}].Close()
	// Then the port can be allocated again.
	assert.Equal(t, lowerPort, hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey2))
	assert.True(t, hostPortRange.isAllocated(lowerPort))
}

func TestSameCacheKeyAllocatedMultipleTimes(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"

	hostPortRange := createCachedHostPortRange(lowerPort, 1026)
	{
		allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
		assert.True(t, allocatedPort >= lowerPort)
		allocatedPort2 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
		assert.True(t, allocatedPort2 >= lowerPort)
		assert.NotEqual(t, allocatedPort, allocatedPort2)
		// Release the port which goes to the same cache entry.
		hostPortRange.releasePort(allocatedPort, portCacheKey)
		assert.False(t, hostPortRange.isAllocated(allocatedPort))
		// Release the port again to the same cache entry.
		hostPortRange.releasePort(allocatedPort2, portCacheKey)
		assert.False(t, hostPortRange.isAllocated(allocatedPort2))
	}
	{
		// Allocate a new port which should be from the cache entry for portCacheKey
		allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
		assert.True(t, allocatedPort >= lowerPort)
		// Allocate a new port which should be from the LRU list.
		allocatedPort2 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, "cache2")
		assert.True(t, allocatedPort2 >= lowerPort)
		assert.NotEqual(t, allocatedPort, allocatedPort2)
	}
}

func TestBypassCache(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"

	hostPortRange := createCachedHostPortRange(lowerPort, 1026)
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.True(t, allocatedPort >= lowerPort)
	// Allocate directly
	allocatedPort2 := hostPortRange.getFreePort(opener, v1.ProtocolTCP, "")
	assert.True(t, allocatedPort2 >= lowerPort)
	assert.NotEqual(t, allocatedPort, allocatedPort2)
	// No available ports left now
	assert.True(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, "") < 0)
	// Release the port which goes to the same cache entry.
	hostPortRange.releasePort(allocatedPort, portCacheKey)
	// Allocate the port from the cache although it indicates to bypass the cache.
	assert.Equal(t, allocatedPort, hostPortRange.getFreePort(opener, v1.ProtocolTCP, ""))
	assert.True(t, hostPortRange.isAllocated(allocatedPort))

	// The release should bypass the cache too.
	hostPortRange.releasePort(allocatedPort, "")
	assert.False(t, hostPortRange.isAllocated(allocatedPort))
	assert.Empty(t, hostPortRange.cache)
}

func TestReleaseDuplicatePorts(t *testing.T) {
	socketManager := NewFakeSocketManager()
	opener := socketManager.openFakeSocket
	var lowerPort int32 = 1024
	portCacheKey := "cache"

	hostPortRange := createCachedHostPortRange(lowerPort, 1025)
	hostPortRange.releasePort(lowerPort, "")
	hostPortRange.releasePort(1025, "")
	allocatedPort := hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey)
	assert.Equal(t, lowerPort, allocatedPort)
	assert.False(t, hostPortRange.getFreePort(opener, v1.ProtocolTCP, portCacheKey) >= lowerPort)
}

func TestAllocateReleaseWithCacheInParallel(t *testing.T) {
	t.Parallel()
	opener := NewFakeSocketManager().openFakeSocket
	var upperPort int32 = 1200
	var lowerPort int32 = 1025
	hostPortRange := createCachedHostPortRange(lowerPort, upperPort)

	runTimes := 512
	wg := sync.WaitGroup{}
	wg.Add(runTimes)
	for i := 0; i < runTimes; i += 1 {
		go func(index int) {
			cacheKey := fmt.Sprintf("container-%d", index % 3)
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			var port int32
			for {
				if port = hostPortRange.getFreePort(opener, v1.ProtocolTCP, cacheKey); port > 0 {
					assert.True(t, port >= lowerPort && port < upperPort)
					break
				}
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			hostPortRange.releasePort(port, cacheKey)
		}(i)
	}
	wg.Wait()

	for port := lowerPort; port < upperPort; port += 1 {
		assert.False(t, hostPortRange.isAllocated(port), "Port %d was not released")
	}
}
