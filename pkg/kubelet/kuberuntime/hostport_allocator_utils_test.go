package kuberuntime

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"testing"
)

func TestNormalizeProtocol(t *testing.T) {
	assert.Equal(t, v1.ProtocolTCP, normalizePortProtocol(v1.ProtocolTCP))
	assert.Equal(t, v1.ProtocolUDP, normalizePortProtocol(v1.ProtocolUDP))
	assert.Equal(t, v1.ProtocolTCP, normalizePortProtocol(""))
}

func TestParseNetTOS(t *testing.T) {
	assert.Equal(t, NETTOS_INVALID, parseNetTOS("UNKNOWN", NETTOS_INVALID))
	assert.Equal(t, NETTOS_BE, parseNetTOS("UNKNOWN", NETTOS_BE))
	assert.Equal(t, NETTOS_BE, parseNetTOS("BE", NETTOS_INVALID))
	assert.Equal(t, NETTOS_AF1, parseNetTOS("AF1", NETTOS_BE))
	assert.Equal(t, NETTOS_AF4, parseNetTOS("AF4", NETTOS_BE))
}

func TestParseContainerHostPort(t *testing.T) {
	testCases := []struct {
		env        v1.EnvVar
		outputPort *containerHostPort
	}{
		{
			env: v1.EnvVar{
				Name: "unknown",
				Value: "AF4",
			},
			outputPort: nil,
		},
		{
			env: v1.EnvVar{
				Name: "PORTALLOC_TCP_port1",
				Value: "unknown",
			},
			outputPort: nil,
		},
		{
			env: v1.EnvVar{
				Name: "PORTALLOC_UNK_port1",
				Value: "BE",
			},
			outputPort: nil,
		},
		{
			env: v1.EnvVar{
				Name: "PORTALLOC_UDP_port1",
				Value: "AF4",
			},
			outputPort: &containerHostPort{
				Port: 0,
				Tos: NETTOS_AF4,
				Proto: "UDP",
			},
		},
		{
			env: v1.EnvVar{
				Name: "PORTALLOC_TCP_important",
				Value: "AF4",
			},
			outputPort: &containerHostPort{
				Port: 0,
				Tos: NETTOS_AF4,
				Proto: "TCP",
			},
		},
	}

	for _, tc := range testCases {
		hostPort := parseContainerHostPortFromEnv(&tc.env)
		if tc.outputPort == nil {
			assert.Nil(t, hostPort)
		} else {
			assert.Equal(t, *tc.outputPort, *hostPort)
		}
	}
}

func TestParseHostPortRangeSpecs(t *testing.T) {
	testCases := []struct {
		spec           string
		portRangeSpecs map[NetTOS]hostPortRangeSpec
	}{
		{
			spec: "",
			portRangeSpecs: map[NetTOS]hostPortRangeSpec{},
		},
		{
			spec: "AF4:1025-1200,BE:20000-21000",
			portRangeSpecs: map[NetTOS]hostPortRangeSpec{
				NETTOS_AF4: hostPortRangeSpec{1025, 1200},
				NETTOS_BE: hostPortRangeSpec{20000, 21000},
			},
		},
		{
			spec: "AF4:1025-1024,BE:20000-21000",
			portRangeSpecs: map[NetTOS]hostPortRangeSpec{
				NETTOS_BE: hostPortRangeSpec{20000, 21000},
			},
		},
		{
			spec: "AF4:1025-1028f,BE:20000-21000",
			portRangeSpecs: map[NetTOS]hostPortRangeSpec{
				NETTOS_BE: hostPortRangeSpec{20000, 21000},
			},
		},
		{
			spec: "UNKNOWN:1025-1028,,BE:20000-21000",
			portRangeSpecs: map[NetTOS]hostPortRangeSpec{
				NETTOS_BE: hostPortRangeSpec{20000, 21000},
			},
		},
	}

	for _, tc := range testCases {
		portRangeSpecs := parseHostPortRangeSpecs(tc.spec)
		assert.Equal(t, tc.portRangeSpecs, portRangeSpecs)
	}
}
