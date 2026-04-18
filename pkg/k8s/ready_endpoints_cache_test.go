package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	discov1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

// --- WaitForReady tests ---

func TestWaitForReady_AlreadyReady(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	cache.Update(key, []*discov1.EndpointSlice{
		newReadySlice("testns", "testsvc", "1.2.3.4"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	isColdStart, err := cache.WaitForReady(ctx, key)
	r.NoError(err)
	r.False(isColdStart, "should not be a cold start when already ready")
}

func TestWaitForReady_TimesOut(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	isColdStart, err := cache.WaitForReady(ctx, key)
	r.Error(err)
	r.False(isColdStart)
	r.ErrorIs(err, context.DeadlineExceeded)
	r.Contains(err.Error(), key, "error should mention the service key")
}

func TestWaitForReady_ColdStart(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cache.Update(key, []*discov1.EndpointSlice{
			newReadySlice("testns", "testsvc", "1.2.3.4"),
		})
	}()

	isColdStart, err := cache.WaitForReady(ctx, key)
	r.NoError(err)
	r.True(isColdStart, "should be a cold start when we had to wait")
}

func TestWaitForReady_IgnoresUnrelatedBroadcast(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"
	const otherKey = "testns/othersvc"

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cache.Update(otherKey, []*discov1.EndpointSlice{
			newReadySlice("testns", "othersvc", "5.6.7.8"),
		})
		time.Sleep(50 * time.Millisecond)
		cache.Update(key, []*discov1.EndpointSlice{
			newReadySlice("testns", "testsvc", "1.2.3.4"),
		})
	}()

	isColdStart, err := cache.WaitForReady(ctx, key)
	r.NoError(err)
	r.True(isColdStart)
}

func TestWaitForReady_ContextCancelled(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	isColdStart, err := cache.WaitForReady(ctx, key)
	r.Error(err)
	r.False(isColdStart)
	r.ErrorIs(err, context.Canceled)
}

// --- PickReadyEndpoint tests ---

func TestPickReadyEndpoint_ReturnsReadyIPAndPort(t *testing.T) {
	r := require.New(t)
	c := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	port := int32(8080)
	c.Update(key, []*discov1.EndpointSlice{
		{
			Ports:     []discov1.EndpointPort{{Port: &port}},
			Endpoints: []discov1.Endpoint{{Addresses: []string{"1.2.3.4"}}},
		},
	})

	// unnamed port (Name==nil) is stored under the "" key
	ip, gotPort, ok := c.PickReadyEndpoint(key, "")
	r.True(ok)
	r.Equal("1.2.3.4", ip)
	r.Equal(int32(8080), gotPort)
}

func TestPickReadyEndpoint_EmptyCache(t *testing.T) {
	r := require.New(t)
	c := NewReadyEndpointsCache(logr.Discard())

	_, _, ok := c.PickReadyEndpoint("testns/testsvc", "")
	r.False(ok, "should return false for unknown service")
}

func TestPickReadyEndpoint_ReturnsIPFromSet(t *testing.T) {
	r := require.New(t)
	c := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	port := int32(8080)
	c.Update(key, []*discov1.EndpointSlice{
		{
			Ports: []discov1.EndpointPort{{Port: &port}},
			Endpoints: []discov1.Endpoint{
				{Addresses: []string{"1.2.3.4"}},
				{Addresses: []string{"5.6.7.8"}},
				{Addresses: []string{"9.10.11.12"}},
			},
		},
	})

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		ip, _, ok := c.PickReadyEndpoint(key, "")
		r.True(ok)
		seen[ip] = true
	}
	for ip := range seen {
		r.Contains([]string{"1.2.3.4", "5.6.7.8", "9.10.11.12"}, ip)
	}
}

func TestPickReadyEndpoint_PortResolution(t *testing.T) {
	httpPort := int32(8080)
	metricsPort := int32(9090)
	httpName := "http"
	metricsName := "metrics"

	slice := &discov1.EndpointSlice{
		Ports: []discov1.EndpointPort{
			{Name: &httpName, Port: &httpPort},
			{Name: &metricsName, Port: &metricsPort},
		},
		Endpoints: []discov1.Endpoint{{Addresses: []string{"1.2.3.4"}}},
	}

	tests := map[string]struct {
		portName string
		wantPort int32
		wantOK   bool
	}{
		"named port present": {
			portName: "http",
			wantPort: 8080,
			wantOK:   true,
		},
		"named port absent": {
			portName: "admin",
			wantOK:   false,
		},
		"empty portName with only named ports": {
			portName: "",
			wantOK:   false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			c := NewReadyEndpointsCache(logr.Discard())
			c.Update("testns/testsvc", []*discov1.EndpointSlice{slice})

			_, gotPort, ok := c.PickReadyEndpoint("testns/testsvc", tc.portName)
			r.Equal(tc.wantOK, ok)
			if tc.wantOK {
				r.Equal(tc.wantPort, gotPort)
			}
		})
	}
}

func TestPickReadyEndpoint_UnnamedPortWithEmptyPortName(t *testing.T) {
	r := require.New(t)
	c := NewReadyEndpointsCache(logr.Discard())

	port := int32(8080)
	c.Update("testns/testsvc", []*discov1.EndpointSlice{
		{
			Ports:     []discov1.EndpointPort{{Port: &port}}, // Name is nil → stored as ""
			Endpoints: []discov1.Endpoint{{Addresses: []string{"1.2.3.4"}}},
		},
	})

	_, gotPort, ok := c.PickReadyEndpoint("testns/testsvc", "")
	r.True(ok)
	r.Equal(int32(8080), gotPort)
}

// --- collectServiceState tests ---

func TestCollectServiceState_ReadyEndpoints(t *testing.T) {
	r := require.New(t)
	slice := &discov1.EndpointSlice{
		Endpoints: []discov1.Endpoint{
			{Addresses: []string{"1.2.3.4"}},
			{Addresses: []string{"5.6.7.8"}},
		},
	}
	s := collectServiceState([]*discov1.EndpointSlice{slice})
	r.True(s.ready)
	r.ElementsMatch([]string{"1.2.3.4", "5.6.7.8"}, s.readyIPs)
}

func TestCollectServiceState_NotReadyEndpoints(t *testing.T) {
	r := require.New(t)
	notReady := false
	slice := &discov1.EndpointSlice{
		Endpoints: []discov1.Endpoint{
			{
				Addresses:  []string{"1.2.3.4"},
				Conditions: discov1.EndpointConditions{Ready: &notReady},
			},
		},
	}
	s := collectServiceState([]*discov1.EndpointSlice{slice})
	r.False(s.ready)
}

func TestCollectServiceState_NilReadyTreatedAsReady(t *testing.T) {
	r := require.New(t)
	slice := &discov1.EndpointSlice{
		Endpoints: []discov1.Endpoint{
			{Addresses: []string{"1.2.3.4"}},
		},
	}
	s := collectServiceState([]*discov1.EndpointSlice{slice})
	r.True(s.ready, "nil Ready should be treated as ready per K8s API spec")
}

func TestCollectServiceState_ExtractsPort(t *testing.T) {
	r := require.New(t)
	slice := &discov1.EndpointSlice{
		Ports: []discov1.EndpointPort{
			{Port: ptr.To(int32(8080))},
		},
		Endpoints: []discov1.Endpoint{
			{Addresses: []string{"1.2.3.4"}},
		},
	}
	s := collectServiceState([]*discov1.EndpointSlice{slice})
	r.Equal(int32(8080), s.ports[""])
}

func TestCollectServiceState_DeduplicatesIPs(t *testing.T) {
	r := require.New(t)
	// Two slices with overlapping IPs.
	s := collectServiceState([]*discov1.EndpointSlice{
		{Endpoints: []discov1.Endpoint{{Addresses: []string{"1.2.3.4"}}}},
		{Endpoints: []discov1.Endpoint{{Addresses: []string{"1.2.3.4", "5.6.7.8"}}}},
	})
	r.True(s.ready)
	r.Len(s.readyIPs, 2)
}

// --- Update tests ---

func TestUpdate_ClearsStateOnEmptySlices(t *testing.T) {
	r := require.New(t)
	c := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	c.Update(key, []*discov1.EndpointSlice{
		newReadySlice("testns", "testsvc", "1.2.3.4"),
	})

	c.Update(key, nil) // clear

	r.False(c.HasReadyEndpoints(key))
	_, _, ok := c.PickReadyEndpoint(key, "")
	r.False(ok)
}

func TestUpdateDeletesKeyWhenNoSlices(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	cache.Update(key, []*discov1.EndpointSlice{
		newReadySlice("testns", "testsvc", "1.2.3.4"),
	})

	r.True(cache.HasReadyEndpoints(key))
	_, ok := cache.states.Load(key)
	r.True(ok, "key should exist after update with slices")

	cache.Update(key, nil)

	r.False(cache.HasReadyEndpoints(key))
	_, ok = cache.states.Load(key)
	r.False(ok, "key should be removed when service has no slices")
}

func TestUpdateRetainsKeyForNonReadySlices(t *testing.T) {
	r := require.New(t)
	cache := NewReadyEndpointsCache(logr.Discard())
	const key = "testns/testsvc"

	notReady := false
	cache.Update(key, []*discov1.EndpointSlice{
		{
			Endpoints: []discov1.Endpoint{
				{
					Addresses:  []string{"1.2.3.4"},
					Conditions: discov1.EndpointConditions{Ready: &notReady},
				},
			},
		},
	})

	r.False(cache.HasReadyEndpoints(key))
	_, ok := cache.states.Load(key)
	r.True(ok, "key should remain when slices exist but none are ready")
}

// --- endpointSliceFromDeleteObj tests ---

func TestEndpointSliceFromDeleteObj_DirectObject(t *testing.T) {
	r := require.New(t)
	slice := &discov1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-slice",
			Namespace: "testns",
		},
	}

	got, err := endpointSliceFromDeleteObj(slice)
	r.NoError(err)
	r.Equal(slice, got)
}

func TestEndpointSliceFromDeleteObj_TombstoneValue(t *testing.T) {
	r := require.New(t)
	slice := &discov1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-slice",
			Namespace: "testns",
		},
	}

	got, err := endpointSliceFromDeleteObj(cache.DeletedFinalStateUnknown{Obj: slice})
	r.NoError(err)
	r.Equal(slice, got)
}

func TestEndpointSliceFromDeleteObj_InvalidTombstonePayload(t *testing.T) {
	r := require.New(t)

	_, err := endpointSliceFromDeleteObj(cache.DeletedFinalStateUnknown{Obj: "not-an-endpointslice"})
	r.Error(err)
}

// --- helpers ---

func newReadySlice(namespace, service string, addresses ...string) *discov1.EndpointSlice {
	endpoints := make([]discov1.Endpoint, 0, len(addresses))
	for _, addr := range addresses {
		endpoints = append(endpoints, discov1.Endpoint{
			Addresses: []string{addr},
		})
	}

	return &discov1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      service + "-slice",
			Namespace: namespace,
			Labels: map[string]string{
				discov1.LabelServiceName: service,
			},
		},
		Endpoints: endpoints,
	}
}
