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
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	runtimeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1/runtime"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

type containerHostPortsState struct {
	// Maps from the container id to the corresponding allocated container host ports.
	hostPorts map[podContainerId]map[string]containerHostPort
}

func (containerState *containerHostPortsState) setHostPorts(containerId *podContainerId, hostPorts map[string]containerHostPort) {
	containerState.hostPorts[*containerId] = hostPorts
}

func (containerState *containerHostPortsState) getAndDeleteHostPorts(containerId *podContainerId) map[string]containerHostPort {
	if containerPorts, existed := containerState.hostPorts[*containerId]; existed {
		delete(containerState.hostPorts, *containerId)
		return containerPorts
	}
	return nil
}

type hostPortRangeKey struct {
	tos NetTOS
	proto v1.Protocol
}

// Encapsulates the host port allocation state in this node.
type HostPortAllocator struct {
	sync.RWMutex
	// Maps from the network TOS and protocol to the corresponding port range.
	hostPortRangeMap map[hostPortRangeKey]*cachedHostPortRange
	// Maps from a container to its allocated host ports.
	containerState *containerHostPortsState
	// The function to open a local port to verify the port is not currently occupied.
	portOpener hostportOpener
}

// Creates a host port allocator.
func NewHostPortAllocator(portRangeSpec string, portOpener hostportOpener) *HostPortAllocator {
	glog.Infof("[hostportmanager] creating new in-memory port state")
	tosPortRangeSpecs := parseHostPortRangeSpecs(portRangeSpec)
	hostPortAllocator := &HostPortAllocator{
		hostPortRangeMap: make(map[hostPortRangeKey]*cachedHostPortRange),
		containerState: &containerHostPortsState{hostPorts: make(map[podContainerId]map[string]containerHostPort)},
		portOpener: portOpener,
	}
	if hostPortAllocator.portOpener == nil {
		hostPortAllocator.portOpener = openLocalPort
	}
	tosPortRanges := hostPortAllocator.hostPortRangeMap
	for tos, rangeSpec := range (tosPortRangeSpecs) {
		tosPortRanges[hostPortRangeKey{tos: tos, proto: v1.ProtocolTCP}] = createCachedHostPortRange(rangeSpec.lowerPort, rangeSpec.upperPort)
		tosPortRanges[hostPortRangeKey{tos: tos, proto: v1.ProtocolUDP}] = createCachedHostPortRange(rangeSpec.lowerPort, rangeSpec.upperPort)
	}
	// TODO(wensheng): check or overwrite the sysctl directives to reserve the ports.
	return hostPortAllocator
}

// Returns whether the host port allocation is enabled.
func (hostPortAllocator *HostPortAllocator) isEnabled() bool {
	return hostPortAllocator != nil && len(hostPortAllocator.hostPortRangeMap) > 0
}

// Initializes the HostPortAllocator from the current running kubernetes containers.
// This should only be called when the kubelet is started at the beginning.
func (hostPortAllocator *HostPortAllocator) Init(runningContainers []*runtimeapi.Container) error {
	if !hostPortAllocator.isEnabled() {
		return nil
	}

	glog.Infof("[hostportmanager] initializing new in-memory port state")
	// Maps from host port range to the already allocated ports.
	allocatedPortsMap := make(map[hostPortRangeKey]map[int32]bool)
	for _, container := range runningContainers {
		if container == nil {
			continue
		}
		if container.State != runtimeapi.ContainerState_CONTAINER_RUNNING && container.State != runtimeapi.ContainerState_CONTAINER_CREATED {
			continue
		}

		allocatedHostPorts := make(map[string]containerHostPort)
		if found, err := getJSONObjectFromLabel(container.GetAnnotations(), kAllocatedHostPortAnnotationLabel, &allocatedHostPorts); err != nil || !found {
			if err != nil {
				glog.Warningf("Failed to parse %q from annotations %q: %v", kAllocatedHostPortAnnotationLabel, container.GetAnnotations(), err)
			}
			continue
		}
		labeledContainerInfo := getContainerInfoFromLabels(container.GetLabels())
		if labeledContainerInfo == nil {
			glog.Warningf("Failed to parse the labels for container %q", container.Id)
			continue
		}

		glog.V(1).Infof("Try to parse the host ports for running container %s", container.Id)
		allHostPorts := make(map[string]containerHostPort)
		for name, hostPort := range allocatedHostPorts {
			if hostPort.Valid() {
				glog.Infof("Add host port %s allocated for container %s", hostPort, container.Id)
				allHostPorts[name] = hostPort
				portRangeKey :=	hostPortRangeKey{
					tos: hostPort.Tos,
					proto: normalizePortProtocol(hostPort.Proto),
				}
				if _, ok := hostPortAllocator.hostPortRangeMap[portRangeKey]; ok {
					if _, ok := allocatedPortsMap[portRangeKey]; !ok {
						allocatedPortsMap[portRangeKey] = make(map[int32]bool)
					}
					allocatedPortsMap[portRangeKey][hostPort.Port] = true
				}
			}
		}

		if len(allHostPorts) > 0 {
			hostPortAllocator.containerState.setHostPorts(&podContainerId{
				namespace: labeledContainerInfo.PodNamespace,
				podName: labeledContainerInfo.PodName,
				containerName: labeledContainerInfo.ContainerName,
			}, allHostPorts)
		}
	}

	for portRangeKey, allocatedPorts := range(allocatedPortsMap) {
		if len(allocatedPorts) >= 0 {
			hostPortAllocator.hostPortRangeMap[portRangeKey].Initialize(allocatedPorts)
		}
	}

	return nil
}

func (hostPortAllocator *HostPortAllocator) isPortAllocated(hostPort containerHostPort) bool {
	if portRange, ok := hostPortAllocator.hostPortRangeMap[hostPortRangeKey{
		tos: hostPort.Tos,
		proto: normalizePortProtocol(hostPort.Proto),
	}]; ok && portRange != nil {
		return portRange.isAllocated(hostPort.Port)
	}
	return false
}

func (hostPortAllocator *HostPortAllocator) releasePort(hostPort containerHostPort, portCacheKey string) {
	if portRange, ok := hostPortAllocator.hostPortRangeMap[hostPortRangeKey{
		tos: hostPort.Tos,
		proto: normalizePortProtocol(hostPort.Proto),
	}]; ok && portRange != nil {
		portRange.releasePort(hostPort.Port, portCacheKey)
	}
}

func (hostPortAllocator *HostPortAllocator) allocatePort(tos NetTOS, proto v1.Protocol, portCacheKey string) int32 {
	normalizedProto := normalizePortProtocol(proto)
	if portRange, ok := hostPortAllocator.hostPortRangeMap[hostPortRangeKey{
		tos: tos, proto: normalizedProto,
	}]; ok && portRange != nil {
		return portRange.getFreePort(hostPortAllocator.portOpener, normalizedProto, portCacheKey)
	}
	return -1
}

// Updates the container options given the allocated host ports for the container.
// The container options are used to create the underlying containers in the host through CRI.
func MaybeUpdateContainerOptions(allocatedHostPorts map[string]containerHostPort, containerName string, containerEnvs []kubecontainer.EnvVar) (string, string) {
	if len(allocatedHostPorts) <= 0 || len(containerEnvs) <= 0 {
		return "", ""
	}

	updated := false
	// Update the Env Vars to indicate the allocated host ports.
	for idx := range(containerEnvs) {
		if allocatedHostPort, ok := allocatedHostPorts[containerEnvs[idx].Name]; ok {
			if allocatedHostPort.Port > 0 {
				updated = true
				containerEnvs[idx].Value = strconv.Itoa(int(allocatedHostPort.Port))
			}
		}
	}

	// Update the container ports annotation so that it can be persisted across the kubelet restarts.
	if updated {
		rawHostPorts, err := json.Marshal(allocatedHostPorts)
		if err != nil {
			glog.Errorf("Unable to marshal allocated host ports for container %q: %v", containerName, err)
		} else {
			return kAllocatedHostPortAnnotationLabel, string(rawHostPorts)
		}
	}
	return "", ""
}

// Try to allocate the host ports for the given pod container.
func (hostPortAllocator *HostPortAllocator) AllocatePortsForContainer(pod *v1.Pod, container *v1.Container) (map[string]containerHostPort, error) {
	// Only if the pod is using host network, the host port allocation is meaningful.
	if !hostPortAllocator.isEnabled() || container == nil || pod == nil || !kubecontainer.IsHostNetworkPod(pod) {
		return nil, nil
	}

	// Release the allocated ports for the container if any first.
	hostPortAllocator.ReleasePortsForContainer(pod, container.Name)

	fullContainerSpec := podContainerId{namespace: pod.Namespace, podName: pod.Name, containerName: container.Name}
	allocatedHostPorts := make(map[string]containerHostPort)
	hostPortAllocator.Lock()
	defer hostPortAllocator.Unlock()
	var allocationError error = nil
	for _, env := range container.Env {
		if hostPort := parseContainerHostPortFromEnv(&env); hostPort != nil && hostPort.Port == 0 {
			if _, existed := allocatedHostPorts[env.Name]; existed {
				glog.Warningf("Container %s has duplicate port name %s, deduping the host port allocation", container.Name, env.Name)
				continue
			}
			portCacheKey := getContainerPortCacheKey(&fullContainerSpec, env.Name)
			allocatedPort := hostPortAllocator.allocatePort(hostPort.Tos, hostPort.Proto, portCacheKey)
			if (allocatedPort > 0) {
				hostPort.Port = allocatedPort
				glog.Infof("Allocates host port %s for container %s", hostPort, fullContainerSpec)
			} else {
				glog.Warningf("Failed to allocate host port %s for container %s", hostPort, fullContainerSpec)
				allocationError = fmt.Errorf("Cannot allocate all the host ports for container %s", container.Name)
				break
			}
			allocatedHostPorts[env.Name] = *hostPort
		}
	}
	// If the allocation failed, revert the partially allocated ports.
	if allocationError != nil {
		// Release the already allocated ports.
		for _, hostPort := range allocatedHostPorts {
			glog.V(1).Infof("Deallocates the already allocated host port %s", hostPort)
			hostPortAllocator.releasePort(hostPort, /*portCacheKey=*/"")
		}
		return nil, allocationError
	}

	if len(allocatedHostPorts) > 0 {
		hostPortAllocator.containerState.setHostPorts(&fullContainerSpec, allocatedHostPorts)
	}
	// Copy the allocated host ports to avoid leaking the internal data structure.
	allocationCopy := make(map[string]containerHostPort)
	for key, value := range(allocatedHostPorts) {
		allocationCopy[key] = value
	}
	return allocationCopy, nil
}

func (hostPortAllocator *HostPortAllocator) ReleasePortsForContainer(pod *v1.Pod, containerName string) error {
	if !hostPortAllocator.isEnabled() || len(containerName) <= 0 || pod == nil || !kubecontainer.IsHostNetworkPod(pod) {
		return nil
	}
	hostPortAllocator.Lock()
	defer hostPortAllocator.Unlock()
	fullContainerSpec := &podContainerId{namespace: pod.Namespace, podName:pod.Name, containerName: containerName}
	allHostPorts := hostPortAllocator.containerState.getAndDeleteHostPorts(fullContainerSpec)
	for name, hostPort := range allHostPorts {
		glog.V(1).Infof("Deallocates the host port %s for container %v", hostPort, fullContainerSpec)
		hostPortAllocator.releasePort(hostPort, getContainerPortCacheKey(fullContainerSpec, name))
	}
	return nil
}
