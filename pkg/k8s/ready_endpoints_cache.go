package k8s

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	discov1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// serviceState is an immutable snapshot of a service's ready pod IPs.
// Replaced atomically on every EndpointSlice event; readers hold a pointer
// to the old snapshot safely without locks.
type serviceState struct {
	ready    bool             // len(readyIPs) > 0
	readyIPs []string         // K8s-ready pod IPs (deduplicated)
	ports    map[string]int32 // portName → containerPort from EndpointSlice
}

func (s *serviceState) hasReady() bool { return s != nil && s.ready }

// ReadyEndpointsCache maintains a derived map of service -> serviceState
// for O(1) hot-path lookups, plus a broadcast mechanism so the cold-start
// wait function can block until a service becomes ready.
type ReadyEndpointsCache struct {
	lggr logr.Logger

	// "namespace/service" -> *atomic.Pointer[serviceState]
	states sync.Map

	// Broadcast mechanism: the channel is closed on any change,
	// then replaced with a fresh one. Waiters select on the channel.
	mu       sync.Mutex
	notifyCh chan struct{}
}

// NewReadyEndpointsCache creates a new empty ready endpoints cache.
func NewReadyEndpointsCache(lggr logr.Logger) *ReadyEndpointsCache {
	return &ReadyEndpointsCache{
		lggr:     lggr.WithName("readyEndpointsCache"),
		notifyCh: make(chan struct{}),
	}
}

// HasReadyEndpoints returns true if the service has at least one ready endpoint.
// This is the fast hot-path check (one atomic load).
func (c *ReadyEndpointsCache) HasReadyEndpoints(serviceKey string) bool {
	if v, ok := c.states.Load(serviceKey); ok {
		return v.(*atomic.Pointer[serviceState]).Load().hasReady()
	}
	return false
}

// WaitForReady waits until the service has at least one ready endpoint or
// the context is cancelled/timed out.
// Returns:
//   - (false, nil)  — warm backend, already ready (fast path)
//   - (true, nil)   — cold start, but backend became ready
//   - (false, error) — context cancelled or timed out
func (c *ReadyEndpointsCache) WaitForReady(ctx context.Context, serviceKey string) (isColdStart bool, err error) {
	if c.HasReadyEndpoints(serviceKey) {
		return false, nil
	}

	// Get the current notification channel before re-checking
	c.mu.Lock()
	ch := c.notifyCh
	c.mu.Unlock()

	// Re-check after getting the channel (close the race window).
	// Return isColdStart=false: we never actually blocked, so this
	// is still the warm/fast path.
	if c.HasReadyEndpoints(serviceKey) {
		return false, nil
	}

	c.lggr.V(1).Info("cold-start: waiting for ready endpoints", "key", serviceKey)

	for {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf(
				"context done while waiting for ready endpoints for %s: %w",
				serviceKey, ctx.Err(),
			)

		case <-ch:
			if c.HasReadyEndpoints(serviceKey) {
				c.lggr.Info("cold-start: endpoints became ready", "key", serviceKey)
				return true, nil
			}
			// Not our service — get the new channel and re-check
			// before waiting again to avoid missing a broadcast
			// that fired between the check above and now.
			c.mu.Lock()
			ch = c.notifyCh
			c.mu.Unlock()
			if c.HasReadyEndpoints(serviceKey) {
				c.lggr.Info("cold-start: endpoints became ready", "key", serviceKey)
				return true, nil
			}
		}
	}
}

// PickReadyEndpoint returns a randomly selected ready pod IP and its container
// port for the given portName. portName must match the EndpointSlice port name
// (which equals the Service port name). Returns ok=false when no ready pod IPs
// are known for the service or the portName is not found in the port map.
func (c *ReadyEndpointsCache) PickReadyEndpoint(serviceKey, portName string) (ip string, port int32, ok bool) {
	v, exists := c.states.Load(serviceKey)
	if !exists {
		return "", 0, false
	}
	state := v.(*atomic.Pointer[serviceState]).Load()
	if !state.hasReady() {
		return "", 0, false
	}
	port, ok = state.ports[portName]
	if !ok {
		return "", 0, false
	}
	return state.readyIPs[rand.Intn(len(state.readyIPs))], port, true
}

// Update checks the given EndpointSlices, builds a new serviceState snapshot,
// and atomically replaces the old one.
func (c *ReadyEndpointsCache) Update(serviceKey string, endpointSlices []*discov1.EndpointSlice) {
	if len(endpointSlices) == 0 {
		c.states.Delete(serviceKey)
		c.broadcast()
		return
	}

	newState := collectServiceState(endpointSlices)

	ptr := &atomic.Pointer[serviceState]{}
	ptr.Store(newState)
	actual, loaded := c.states.LoadOrStore(serviceKey, ptr)

	if loaded {
		oldState := actual.(*atomic.Pointer[serviceState]).Load()
		actual.(*atomic.Pointer[serviceState]).Store(newState)
		if oldState.hasReady() != newState.hasReady() {
			c.broadcast()
		}
	} else if newState.hasReady() {
		c.broadcast()
	}
}

// broadcast wakes all waiting goroutines by closing the current channel
// and replacing it with a new one.
func (c *ReadyEndpointsCache) broadcast() {
	c.mu.Lock()
	old := c.notifyCh
	c.notifyCh = make(chan struct{})
	c.mu.Unlock()
	close(old)
}

// collectServiceState extracts ready pod IPs and the container port map from a
// set of EndpointSlices into an immutable serviceState snapshot.
// ports maps portName → containerPort; the port name matches Service.spec.ports[i].name.
// Unnamed ports (Name == nil) are stored under the empty string key.
func collectServiceState(slices []*discov1.EndpointSlice) *serviceState {
	seen := make(map[string]struct{})
	ports := make(map[string]int32)
	for _, sl := range slices {
		for _, p := range sl.Ports {
			if p.Port == nil {
				continue
			}
			name := ""
			if p.Name != nil {
				name = *p.Name
			}
			if _, exists := ports[name]; !exists {
				ports[name] = *p.Port
			}
		}
		for i := range sl.Endpoints {
			ep := &sl.Endpoints[i]
			// Kubernetes guarantees that Ready is false for terminating pods, so a
			// separate Terminating check is unnecessary. For services with
			// publishNotReadyAddresses, Ready is always true — we respect that.
			if (ep.Conditions.Ready == nil || *ep.Conditions.Ready) && len(ep.Addresses) > 0 {
				for _, addr := range ep.Addresses {
					seen[addr] = struct{}{}
				}
			}
		}
	}
	ips := make([]string, 0, len(seen))
	for ip := range seen {
		ips = append(ips, ip)
	}
	return &serviceState{ready: len(ips) > 0, readyIPs: ips, ports: ports}
}

// updateReadyCache updates the ready cache for the service that owns
// the given EndpointSlice.
func updateReadyCache(lggr logr.Logger, ctrlCache ctrlcache.Cache, readyCache *ReadyEndpointsCache, slice *discov1.EndpointSlice) {
	svcName, ok := slice.Labels[discov1.LabelServiceName]
	if !ok || svcName == "" {
		return
	}
	ns := slice.Namespace

	list := &discov1.EndpointSliceList{}
	if err := ctrlCache.List(context.Background(), list,
		client.InNamespace(ns),
		client.MatchingLabels{discov1.LabelServiceName: svcName},
	); err != nil {
		lggr.Error(err, "failed to list endpoint slices for ready cache update",
			"namespace", ns, "service", svcName)
		return
	}

	allSlices := make([]*discov1.EndpointSlice, len(list.Items))
	for idx := range list.Items {
		allSlices[idx] = &list.Items[idx]
	}
	readyCache.Update(ns+"/"+svcName, allSlices)
}

func addEvtHandler(lggr logr.Logger, ctrlCache ctrlcache.Cache, readyCache *ReadyEndpointsCache, obj any) {
	eps, ok := obj.(*discov1.EndpointSlice)
	if !ok {
		lggr.Error(
			fmt.Errorf("informer expected EndpointSlice, got %v", obj),
			"skipping event",
		)
		return
	}

	updateReadyCache(lggr, ctrlCache, readyCache, eps)
}

func updateEvtHandler(lggr logr.Logger, ctrlCache ctrlcache.Cache, readyCache *ReadyEndpointsCache, _, newObj any) {
	eps, ok := newObj.(*discov1.EndpointSlice)
	if !ok {
		lggr.Error(
			fmt.Errorf("informer expected EndpointSlice, got %v", newObj),
			"skipping event",
		)
		return
	}

	updateReadyCache(lggr, ctrlCache, readyCache, eps)
}

func deleteEvtHandler(lggr logr.Logger, ctrlCache ctrlcache.Cache, readyCache *ReadyEndpointsCache, obj any) {
	eps, err := endpointSliceFromDeleteObj(obj)
	if err != nil {
		lggr.Error(
			err,
			"skipping event",
		)
		return
	}

	updateReadyCache(lggr, ctrlCache, readyCache, eps)
}

// endpointSliceFromDeleteObj unwraps EndpointSlice delete events from either
// a direct object or a DeletedFinalStateUnknown tombstone.
func endpointSliceFromDeleteObj(obj any) (*discov1.EndpointSlice, error) {
	switch t := obj.(type) {
	case *discov1.EndpointSlice:
		return t, nil
	case cache.DeletedFinalStateUnknown:
		eps, ok := t.Obj.(*discov1.EndpointSlice)
		if !ok {
			return nil, fmt.Errorf("informer expected EndpointSlice in tombstone, got %T", t.Obj)
		}
		return eps, nil
	default:
		return nil, fmt.Errorf("informer expected EndpointSlice, got %T", obj)
	}
}

func NewReadyEndpointsCacheWithInformer(
	ctx context.Context,
	lggr logr.Logger,
	ctrlCache ctrlcache.Cache,
) (*ReadyEndpointsCache, error) {
	informer, err := ctrlCache.GetInformer(ctx, &discov1.EndpointSlice{})
	if err != nil {
		lggr.Error(err, "error getting EndpointSlice informer from controller-runtime cache")
		return nil, err
	}

	readyCache := NewReadyEndpointsCache(lggr)

	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			addEvtHandler(lggr, ctrlCache, readyCache, obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			updateEvtHandler(lggr, ctrlCache, readyCache, oldObj, newObj)
		},
		DeleteFunc: func(obj any) {
			deleteEvtHandler(lggr, ctrlCache, readyCache, obj)
		},
	})
	if err != nil {
		lggr.Error(err, "error adding event handler to EndpointSlice informer")
		return nil, err
	}

	return readyCache, nil
}
