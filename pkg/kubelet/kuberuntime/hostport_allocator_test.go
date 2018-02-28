package kuberuntime

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"math/rand"
	"sync"
	"testing"
	"time"
	runtimeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1/runtime"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"fmt"
	"k8s.io/kubernetes/pkg/kubelet/types"
)

func TestUpdatecontainerOptsWithAllocatedHostPorts(t *testing.T) {
	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1200,BE:20000-21000", opener)
	hostPortAllocator.Init(nil)
	containerName := "container1"

	t.Run("ContainerPorts=0", func(t *testing.T) {
		hostPortsAllocation := map[string]containerHostPort{
			"PORTALLOC_TCP_port3": containerHostPort{Port: 1025},
			"PORTALLOC_UDP_port4": containerHostPort{Port: 20500, Proto: "UDP"},
			"PORTALLOC_unknown": containerHostPort{Port: 20001},
		}
		annotationKey, annotationValue := MaybeUpdateContainerOptions(hostPortsAllocation, containerName, nil)
		assert.Empty(t, annotationKey)
		assert.Empty(t, annotationValue)
	})

	t.Run("ContainerPorts=3", func(t *testing.T) {
		t.Run("Without ports allocated", func(t *testing.T) {
			defaultContainerEnvs := []kubecontainer.EnvVar{
				kubecontainer.EnvVar{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
				kubecontainer.EnvVar{Name: "PORTALLOC_UDP_port4", Value: "BE"},
			}
			updatedContainerEnvs := make([]kubecontainer.EnvVar, len(defaultContainerEnvs))
			copy(updatedContainerEnvs, defaultContainerEnvs)
			annotationKey, annotationValue := MaybeUpdateContainerOptions(nil, containerName, updatedContainerEnvs)
			assert.Empty(t, annotationKey)
			assert.Empty(t, annotationValue)
			assert.Equal(t, defaultContainerEnvs, updatedContainerEnvs)
		})
		t.Run("With both ports allocated", func(t *testing.T) {
			allocatedHostPorts := map[string]containerHostPort{
				"PORTALLOC_TCP_port3": containerHostPort{Port: 1025},
				"PORTALLOC_UDP_port4": containerHostPort{Port: 20500, Proto: "UDP"},
				"PORTALLOC_unknown": containerHostPort{Port: 20001},
			}
			containerEnvs := []kubecontainer.EnvVar{
				kubecontainer.EnvVar{Name: "test", Value: "1024"},
				kubecontainer.EnvVar{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
				kubecontainer.EnvVar{Name: "PORTALLOC_UDP_port4", Value: "BE"},
			}
			annotationKey, annotationValue := MaybeUpdateContainerOptions(allocatedHostPorts, containerName, containerEnvs)
			assert.Equal(t, kAllocatedHostPortAnnotationLabel, annotationKey)
			assert.Equal(t, `{"PORTALLOC_TCP_port3":{"port":1025},"PORTALLOC_UDP_port4":{"port":20500,"proto":"UDP"},"PORTALLOC_unknown":{"port":20001}}`, annotationValue)
			assert.Equal(t, []kubecontainer.EnvVar{
				{Name: "test", Value: "1024"},
				{Name: "PORTALLOC_TCP_port3", Value: "1025"},
				{Name: "PORTALLOC_UDP_port4", Value: "20500"},
			}, containerEnvs)
		})
		t.Run("With one port allocation failed", func(t *testing.T) {
			allocatedHostPorts := map[string]containerHostPort{
				"PORTALLOC_TCP_port3": containerHostPort{Port: 1025},
				"PORTALLOC_UDP_port4": containerHostPort{Port: 0, Proto: "UDP"},
			}
			containerEnvs := []kubecontainer.EnvVar{
				kubecontainer.EnvVar{Name: "test", Value: "1024"},
				kubecontainer.EnvVar{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
				kubecontainer.EnvVar{Name: "PORTALLOC_UDP_port4", Value: "BE"},
			}
			annotationKey, annotationValue := MaybeUpdateContainerOptions(allocatedHostPorts, containerName, containerEnvs)
			assert.Equal(t, kAllocatedHostPortAnnotationLabel, annotationKey)
			assert.Equal(t, `{"PORTALLOC_TCP_port3":{"port":1025},"PORTALLOC_UDP_port4":{"proto":"UDP"}}`, annotationValue)
			assert.Equal(t, []kubecontainer.EnvVar{
				{Name: "test", Value: "1024"},
				{Name: "PORTALLOC_TCP_port3", Value: "1025"},
				{Name: "PORTALLOC_UDP_port4", Value: "BE"},
			}, containerEnvs)
		})
	})
}

func TestInitHostPortAllocator(t *testing.T) {
	opener := NewFakeSocketManager().openFakeSocket
	t.Run("Without running containers", func(t *testing.T) {
		hostPortAllocator := NewHostPortAllocator("AF4:1025-1027,BE:20000-20001", opener)
		hostPortAllocator.Init(nil)
		assert.Empty(t, hostPortAllocator.containerState.hostPorts)
	})

	portsAnnotation0 := `{"PORTALLOC_TCP_port3":{"port":1025},"PORTALLOC_UDP_port4":{"port":0,"proto":"UDP"},"PORTALLOC_unknown":{"port":20001}}`
	portsAnnotation1 := `{"PORTALLOC_TCP_port3":{"proto": "TCP", "tos": 4, "port":1025},"PORTALLOC_UDP_port4":{"port":0,"proto":"UDP"}}`
	portsAnnotation2 := `{"PORTALLOC_TCP_port3":{"proto": "TCP", "tos": 4, "port":1026},"PORTALLOC_UDP_port4":{"port":20000,"proto":"UDP"}}`
	t.Run("With running containers", func(t *testing.T) {
		hostPortAllocator := NewHostPortAllocator("AF4:1025-1027,BE:20000-20001", opener)
		hostPortAllocator.Init([]*runtimeapi.Container{
			// Container which is not running.
			&runtimeapi.Container{
				Id: "Container0",
				State: runtimeapi.ContainerState_CONTAINER_EXITED,
				Annotations: map[string]string{kAllocatedHostPortAnnotationLabel: portsAnnotation1},
				Labels: map[string]string{types.KubernetesContainerNameLabel: "Container0"},
			},
			&runtimeapi.Container{
				Id: "Container1",
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
				Annotations: map[string]string{kAllocatedHostPortAnnotationLabel: portsAnnotation0},
				Labels: map[string]string{types.KubernetesContainerNameLabel: "Container1"},
			},
			&runtimeapi.Container{
				Id: "Container2",
				State: runtimeapi.ContainerState_CONTAINER_CREATED,
				Annotations: map[string]string{kAllocatedHostPortAnnotationLabel: portsAnnotation1},
				Labels: map[string]string{types.KubernetesContainerNameLabel: "Container2"},
			},
			&runtimeapi.Container{
				Id: "Container3",
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
				Annotations: map[string]string{kAllocatedHostPortAnnotationLabel: portsAnnotation2},
				Labels: map[string]string{types.KubernetesContainerNameLabel: "Container3"},
			},
		})
		assert.Equal(t, map[podContainerId]map[string]containerHostPort{
			podContainerId{"", "", "Container2"}: map[string]containerHostPort{"PORTALLOC_TCP_port3": {Tos: NETTOS_AF4, Port: 1025, Proto: "TCP"}},
			podContainerId{"", "", "Container3"}: map[string]containerHostPort{
				"PORTALLOC_TCP_port3": {Tos: NETTOS_AF4, Port: 1026, Proto: "TCP"},
				"PORTALLOC_UDP_port4": {Tos: NETTOS_BE, Port: 20000, Proto: "UDP"},
			},
		}, hostPortAllocator.containerState.hostPorts)
		assert.True(t, hostPortAllocator.isPortAllocated(containerHostPort{Tos: NETTOS_AF4, Proto: v1.ProtocolTCP, Port: 1025}))
		assert.False(t, hostPortAllocator.isPortAllocated(containerHostPort{Tos: NETTOS_AF4, Proto: v1.ProtocolUDP, Port: 1025}))
		assert.True(t, hostPortAllocator.isPortAllocated(containerHostPort{Tos: NETTOS_BE, Proto: v1.ProtocolUDP, Port: 20000}))
		assert.False(t, hostPortAllocator.isPortAllocated(containerHostPort{Tos: NETTOS_BE, Proto: v1.ProtocolTCP, Port: 20000}))
		// Cannot allocate a new port because all the ports have been allocated.
		portCacheKey := ""
		assert.Equal(t, int32(-1), hostPortAllocator.allocatePort(NETTOS_AF4, "TCP", portCacheKey))
		assert.Equal(t, int32(-1), hostPortAllocator.allocatePort(NETTOS_BE, "UDP", portCacheKey))
		assert.Equal(t, int32(20000), hostPortAllocator.allocatePort(NETTOS_BE, "TCP", portCacheKey))
	})
}

func TestAllocatePortsDirectly(t *testing.T) {
	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1027,BE:20000-20001", opener)
	assert := assert.New(t)
	portCacheKey := ""
	assert.Equal(int32(20000), hostPortAllocator.allocatePort(NETTOS_BE, "TCP", portCacheKey))
	assert.Equal(int32(20000), hostPortAllocator.allocatePort(NETTOS_BE, "UDP", portCacheKey))
	assert.Subset([]int32{
		hostPortAllocator.allocatePort(NETTOS_AF4, "UDP", portCacheKey),
		hostPortAllocator.allocatePort(NETTOS_AF4, "UDP", portCacheKey),
	}, []int32{1025, 1026})
	assert.Subset([]int32{
		hostPortAllocator.allocatePort(NETTOS_AF4, "TCP", portCacheKey),
		hostPortAllocator.allocatePort(NETTOS_AF4, "TCP", portCacheKey),
	}, []int32{1025, 1026})
	assert.Equal(int32(-1), hostPortAllocator.allocatePort(NETTOS_BE, "TCP", portCacheKey))
	assert.Equal(int32(-1), hostPortAllocator.allocatePort(NETTOS_AF4, "UDP", portCacheKey))

	// Try to release some ports.
	hostPortAllocator.releasePort(containerHostPort{Tos: NETTOS_AF4, Port: 1026, Proto: "UDP"}, portCacheKey)
	hostPortAllocator.releasePort(containerHostPort{Tos: NETTOS_AF4, Port: 1024, Proto: "TCP"}, portCacheKey)
	hostPortAllocator.releasePort(containerHostPort{Tos: NETTOS_BE, Port: 20001, Proto: "TCP"}, portCacheKey)
	hostPortAllocator.releasePort(containerHostPort{Tos: NETTOS_BE, Port: 20000, Proto: "UDP"}, portCacheKey)
	// Allocate the ports again.
	assert.Equal(int32(1026), hostPortAllocator.allocatePort(NETTOS_AF4, "UDP", portCacheKey))
	assert.Equal(int32(-1), hostPortAllocator.allocatePort(NETTOS_AF4, "TCP", portCacheKey))
	assert.Equal(int32(20000), hostPortAllocator.allocatePort(NETTOS_BE, "UDP", portCacheKey))
	assert.Equal(int32(-1), hostPortAllocator.allocatePort(NETTOS_BE, "TCP", portCacheKey))
}

func TestAllocateAndFreePortsForContainers(t *testing.T) {
	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1026,BE:20000-20001", opener)
	assert := assert.New(t)
	defaultEnvs := []v1.EnvVar{
		{Name: "port1", Value: "1024"},
		{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
		{Name: "PORTALLOC_UDP_port4", Value: "BE"},
	}
	t.Run("Do not allocate if not using host network", func(t *testing.T) {
		pod := v1.Pod{
			Spec: v1.PodSpec{
				HostNetwork: false,
				Containers: []v1.Container{
					{Name: "Container0", Env: defaultEnvs},
				},
			},
		}
		allocationResult, err := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
		assert.NoError(err)
		assert.Empty(allocationResult)
	})

	t.Run("Do host port allocations", func(t *testing.T) {
		expectedAllocatedPorts := map[string]containerHostPort{
			"PORTALLOC_TCP_port3": containerHostPort{Tos: NETTOS_AF4, Port: 1025, Proto: "TCP"},
			"PORTALLOC_UDP_port4": containerHostPort{Tos: NETTOS_BE, Port: 20000, Proto: "UDP"},
		}
		pod := v1.Pod{
			Spec: v1.PodSpec{
				HostNetwork: true,
				Containers: []v1.Container{
					{Name: "Container2", Env: defaultEnvs},
				},
			},
		}
		allocationResult, err := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
		assert.NoError(err)
		assert.Equal(expectedAllocatedPorts, allocationResult)
		t.Run("Allocation result committed", func(t *testing.T) {
			// Cannot allocate the port for new container, as all the ports have been taken.
			newPod := pod
			newPod.Name = "NewPod"
			_, err := hostPortAllocator.AllocatePortsForContainer(&newPod, &newPod.Spec.Containers[0])
			assert.Error(err)

			hostPortAllocator.ReleasePortsForContainer(&pod, pod.Spec.Containers[0].Name)
			// Now can allocate the ports again after they have been released.
			allocationResult2, err2 := hostPortAllocator.AllocatePortsForContainer(&newPod, &newPod.Spec.Containers[0])
			assert.NoError(err2)
			assert.Equal(expectedAllocatedPorts, allocationResult2)
			hostPortAllocator.ReleasePortsForContainer(&pod, pod.Spec.Containers[0].Name)
		})
		t.Run("Allocation result released", func(t *testing.T) {
			hostPortAllocator.ReleasePortsForContainer(&pod, pod.Spec.Containers[0].Name)
			// Can allocate the same set of ports
			newPod := pod
			newPod.Name = "NewPod"
			allocationResult2, err2 := hostPortAllocator.AllocatePortsForContainer(&newPod, &newPod.Spec.Containers[0])
			assert.NoError(err2)
			assert.Equal(expectedAllocatedPorts, allocationResult2)
		})
	})
}

func TestAllocateAndReleaseInParallel(t *testing.T) {
	t.Parallel()

	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1100,BE:20000-20500", opener)
	assert := assert.New(t)
	defaultEnvs := []v1.EnvVar{
		{Name: "port1", Value: "1024"},
		{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
		{Name: "PORTALLOC_UDP_port4", Value: "BE"},
	}

	runTimes := 256
	wg := sync.WaitGroup{}
	wg.Add(runTimes)
	for i := 0; i < runTimes; i += 1 {
		containerName := fmt.Sprintf("Container-%d", i)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			pod := v1.Pod{
				Spec: v1.PodSpec{
					HostNetwork: true,
					Containers: []v1.Container{
						{Name: containerName, Env: defaultEnvs},
					},
				},
			}
			for {
				allocationResult, err := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
				if err == nil {
					assert.Equal(2, len(allocationResult))
					for _, port := range allocationResult{
						if port.Proto == "UDP" {
							assert.Equal(NETTOS_BE, port.Tos)
							assert.True(port.Port >= 20000)
							assert.True(port.Port < 20500)
						} else {
							assert.Equal(NETTOS_AF4, port.Tos)
							assert.True(port.Port >= 1025)
							assert.True(port.Port < 1100)
						}
					}
					break
				}
				hostPortAllocator.ReleasePortsForContainer(&pod, pod.Spec.Containers[0].Name)
				time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			}
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			hostPortAllocator.ReleasePortsForContainer(&pod, pod.Spec.Containers[0].Name)
		}()
	}
	wg.Wait()
	assert.Empty(hostPortAllocator.containerState.hostPorts)
}

func TestAllocateTwiceForTheSameContainer(t *testing.T) {
	t.Parallel()

	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1027,BE:20000-20002", opener)
	assert := assert.New(t)
	defaultEnvs := []v1.EnvVar{
		{Name: "port1", Value: "1024"},
		{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
		{Name: "PORTALLOC_UDP_port4", Value: "BE"},
	}
	pod := v1.Pod{
		Spec: v1.PodSpec{
			HostNetwork: true,
			Containers: []v1.Container{
				{Name: "Container0", Env: defaultEnvs},
			},
		},
	}
	allocationResult, err := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
	assert.NoError(err)

	// Try to allocate for new container
	newPod := pod
	newPod.Name = "NewPod"
	// Should allocate a new set of ports
	allocationResult2, err2 := hostPortAllocator.AllocatePortsForContainer(&newPod, &newPod.Spec.Containers[0])
	assert.NoError(err2)
	assert.NotEqual(allocationResult, allocationResult2)

	{
		// We can still allocate the ports.
		allocationResult3, err3 := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
		assert.NoError(err3)
		assert.Equal(allocationResult, allocationResult3)
	}

	{
		// We can still allocate the ports after the previous allocated ports have been explicitly released.
		allocationResult3, err3 := hostPortAllocator.AllocatePortsForContainer(&pod, &pod.Spec.Containers[0])
		assert.NoError(err3)
		assert.Equal(allocationResult, allocationResult3)
	}
}

func TestAllocatedHostPortsStickToContainer(t *testing.T) {
	t.Parallel()

	opener := NewFakeSocketManager().openFakeSocket
	hostPortAllocator := NewHostPortAllocator("AF4:1025-1027,BE:20000-20002", opener)
	assert := assert.New(t)
	defaultEnvs := []v1.EnvVar{
		{Name: "port1", Value: "1024"},
		{Name: "PORTALLOC_TCP_port3", Value: "AF4"},
		{Name: "PORTALLOC_UDP_port4", Value: "BE"},
	}
	pod1 := v1.Pod{
		Spec: v1.PodSpec{
			HostNetwork: true,
			Containers: []v1.Container{
				{Name: "Container0", Env: defaultEnvs},
			},
		},
	}
	pod2 := pod1
	pod1.Name = "pod1"
	pod2.Name = "pod2"

	allocationResult1, err1 := hostPortAllocator.AllocatePortsForContainer(&pod1, &pod1.Spec.Containers[0])
	assert.NoError(err1)

	allocationResult2, err2 := hostPortAllocator.AllocatePortsForContainer(&pod2, &pod2.Spec.Containers[0])
	assert.NoError(err2)

	hostPortAllocator.ReleasePortsForContainer(&pod1, pod1.Spec.Containers[0].Name)
	hostPortAllocator.ReleasePortsForContainer(&pod2, pod2.Spec.Containers[0].Name)

	// With caching, pod2 will be allocated with the previous ports allocated to pod2, even it's allocated first.
	allocationResult22, err22 := hostPortAllocator.AllocatePortsForContainer(&pod2, &pod2.Spec.Containers[0])
	assert.NoError(err22)

	allocationResult12, err12 := hostPortAllocator.AllocatePortsForContainer(&pod1, &pod1.Spec.Containers[0])
	assert.NoError(err12)

	assert.Equal(allocationResult1, allocationResult12)
	assert.Equal(allocationResult2, allocationResult22)
}
