package kuberuntime

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
)

type NetTOS int32

// Defines different level of network type of service:
// NETTOS_BE: best effort
// NETTOS_AF1: assured forwarding level 1
// NETTOS_AF4: assured forwarding level 4
// Traffic with higher level TOS will be served better in network
const (
	NETTOS_INVALID NetTOS = -1
	NETTOS_BE NetTOS = 0
	NETTOS_AF1 NetTOS = 1
	NETTOS_AF4 NetTOS = 4
)

const (
	kAllocatedHostPortNamePrefix string = "PORTALLOC_"
	kAllocatedHostPortAnnotationLabel = "io.kubernetes.container.allocated_host_ports"
	kMaxHostPortNumber int32 = 65536
)

// Encapsulates the fields to represent a container host port.
type containerHostPort struct {
	Port  int32 `json:"port,omitempty"`
	Tos   NetTOS `json:"tos,omitempty"`
	Proto v1.Protocol `json:"proto,omitempty"`
}

func (hostPort containerHostPort) String() string {
	return fmt.Sprintf("(port: %v, TOS: %v, proto: %v)", hostPort.Port, hostPort.Tos, hostPort.Proto)
}

// Returns whether the specific container host port is valid.
func (hostPort containerHostPort) Valid() bool {
	return hostPort.Port > 0 && hostPort.Port < kMaxHostPortNumber &&
		hostPort.Tos >= NETTOS_BE && hostPort.Tos <= NETTOS_AF4 &&
		(hostPort.Proto == v1.ProtocolTCP || hostPort.Proto == v1.ProtocolUDP)
}

// Returns the normalized port protocol, specifically empty protocol is default to TCP.
func normalizePortProtocol(inputProto v1.Protocol) v1.Protocol {
	if inputProto == "" {
		return v1.ProtocolTCP
	}
	return inputProto
}

// Parse the NetTOS from the corresponding string representation.
func parseNetTOS(tosName string, defaultTOS NetTOS) NetTOS {
	switch tosName {
	case "BE":
		return NETTOS_BE
	case "AF1":
		return NETTOS_AF1
	case "AF4":
		return NETTOS_AF4
	default:
		return defaultTOS
	}
}

// Given an EnvVar for a container, if it refers to a host port allocation, parse the corresponding containerHostPort
// struct from the EnvVar's name and value. Returns nil if the EnvVar is not for host port allocation.
func parseContainerHostPortFromEnv(containerEnv *v1.EnvVar) *containerHostPort {
	if !strings.HasPrefix(containerEnv.Name, kAllocatedHostPortNamePrefix) {
		return nil
	}
	portName := strings.TrimPrefix(containerEnv.Name, kAllocatedHostPortNamePrefix)
	portProto := v1.ProtocolTCP
	if strings.HasPrefix(portName, "UDP_") {
		portProto = v1.ProtocolUDP
	} else if !strings.HasPrefix(portName, "TCP_") {
		return nil
	}
	portTOS := parseNetTOS(containerEnv.Value, NETTOS_INVALID)
	if portTOS == NETTOS_INVALID {
		return nil
	}
	return &containerHostPort{Port: 0, Tos: portTOS, Proto: portProto}
}

// Specify a range of host ports reservation [lowerPort, upperPort)
type hostPortRangeSpec struct {
	lowerPort int32
	upperPort int32
}

// Parses a port range specification like: <ToS>:<lower_port>-<upper_port>.
func parseOnePortRangeSpecField(field string) (NetTOS, int32, int32) {
	if tosPortRangeFields := strings.Split(field, ":"); len(tosPortRangeFields) == 2 {
		if portTOS := parseNetTOS(tosPortRangeFields[0], NETTOS_INVALID); portTOS != NETTOS_INVALID {
			if portFields := strings.Split(tosPortRangeFields[1], "-"); len(portFields) == 2 {
				if lower, err := strconv.Atoi(portFields[0]); err == nil {
					if upper, err := strconv.Atoi(portFields[1]); err == nil {
						return portTOS, int32(lower), int32(upper)
					}
				}
			}
		}
	}
	return NETTOS_INVALID, -1, -1
}

// Parses host port range specification from the given |portRangeSpec| string.
// A valid |portRangeSpec| has the format: "<ToS1>:<lower_port1>-<upper_port1>,...<ToSN>:<lower_portN>-<upper_portN>".
func parseHostPortRangeSpecs(portRangeSpec string) map[NetTOS]hostPortRangeSpec {
	const (
		kMinValidPort int32 = 1025
		kMaxValidPort int32 = 32768
	)
	spec_fields := strings.Split(portRangeSpec, ",")
	portRangeSpecMap := make(map[NetTOS]hostPortRangeSpec)
	for _, field := range spec_fields {
		portTOS, lower, upper := parseOnePortRangeSpecField(field)
		if portTOS == NETTOS_INVALID || lower < kMinValidPort || upper > kMaxValidPort || lower > upper {
			glog.Warningf("Skip invalid host port range spec: %q", field)
			continue
		}
		if _, ok := portRangeSpecMap[portTOS]; ok {
			glog.Warningf("Duplicate port range spec for TOS %v", portTOS)
			continue
		}
		portRangeSpecMap[portTOS] = hostPortRangeSpec{lowerPort: lower, upperPort: upper}
	}
	return portRangeSpecMap
}

// The data struct should uniquely identify a container in a pod.
type podContainerId struct {
	namespace string
	podName string
	containerName string
}

func (containerId podContainerId) String() string {
	return fmt.Sprintf("%s.%s.%s", containerId.namespace, containerId.podName, containerId.containerName)
}

func getContainerPortCacheKey(containerId *podContainerId, portName string) string {
	return fmt.Sprintf("%s.%s", containerId, portName)
}
