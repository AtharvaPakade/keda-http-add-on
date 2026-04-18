package middleware

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/kedacore/http-add-on/interceptor/handler"
	kedahttp "github.com/kedacore/http-add-on/pkg/http"
	"github.com/kedacore/http-add-on/pkg/k8s"
	"github.com/kedacore/http-add-on/pkg/util"
)

const defaultFallbackReadinessTimeout = 30 * time.Second

type EndpointResolverConfig struct {
	ReadinessTimeout      time.Duration
	EnableColdStartHeader bool
	DirectPodOnColdStart  bool // route to pod IP directly during cold start
}

type EndpointResolver struct {
	next       http.Handler
	readyCache *k8s.ReadyEndpointsCache
	cfg        EndpointResolverConfig
}

// NewEndpointResolver returns a middleware that resolves a ready backend
// endpoint for each request. It waits for at least one endpoint to become
// ready (handling cold starts) and optionally falls back to an alternate
// upstream when the backend does not become ready in time.
func NewEndpointResolver(next http.Handler, readyCache *k8s.ReadyEndpointsCache, cfg EndpointResolverConfig) *EndpointResolver {
	return &EndpointResolver{
		next:       next,
		readyCache: readyCache,
		cfg:        cfg,
	}
}

var _ http.Handler = (*EndpointResolver)(nil)

func (er *EndpointResolver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ir := util.InterceptorRouteFromContext(ctx)

	readinessTimeout := er.cfg.ReadinessTimeout
	// Per-route override from InterceptorRoute spec
	if ir.Spec.Timeouts.Readiness != nil {
		readinessTimeout = ir.Spec.Timeouts.Readiness.Duration
	}

	hasFallback := ir.Spec.ColdStart != nil && ir.Spec.ColdStart.Fallback != nil
	// Bound the readiness wait or otherwise there is no time for the fallback
	if hasFallback && readinessTimeout == 0 {
		readinessTimeout = defaultFallbackReadinessTimeout
	}

	waitCtx := ctx
	if readinessTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, readinessTimeout)
		defer cancel()
	}

	serviceKey := ir.Namespace + "/" + ir.Spec.Target.Service
	isColdStart, err := er.readyCache.WaitForReady(waitCtx, serviceKey)
	if err != nil {
		// No fallback, return an error
		if !hasFallback {
			code := http.StatusBadGateway
			// Context expired or aborted — no time remaining to reach the backend.
			if waitCtx.Err() != nil {
				code = http.StatusGatewayTimeout
			}
			handler.
				NewStatic(code, fmt.Errorf("backend not ready: %w", err)).
				ServeHTTP(w, r)
			return
		}

		// Has fallback but parent context expired, error early
		if ctx.Err() != nil {
			handler.
				NewStatic(http.StatusGatewayTimeout, fmt.Errorf("backend not ready and no time remaining for fallback: %w", err)).
				ServeHTTP(w, r)
			return
		}

		// Fall back to alternate upstream.
		fallbackURL := util.FallbackURLFromContext(ctx)
		ctx = util.ContextWithUpstreamURL(ctx, fallbackURL)
		r = r.WithContext(ctx)
	}

	// isColdStart is only meaningful when the backend resolved without errors
	if err == nil && er.cfg.EnableColdStartHeader {
		w.Header().Set(kedahttp.HeaderColdStart, strconv.FormatBool(isColdStart))
	}

	// Direct-to-pod routing: only during cold start, rewrite the upstream URL
	// to a pod IP so the request bypasses the ClusterIP service. This is useful
	// when the `iptables-min-sync-period` is high.
	if isColdStart && er.cfg.DirectPodOnColdStart {
		if podIP, podPort, ok := er.readyCache.PickReadyEndpoint(serviceKey, ir.Spec.Target.PortName); ok {
			if upstreamURL := util.UpstreamURLFromContext(ctx); upstreamURL != nil {
				podURL := *upstreamURL
				podURL.Host = net.JoinHostPort(podIP, strconv.Itoa(int(podPort)))
				ctx = util.ContextWithUpstreamURL(ctx, &podURL)
				r = r.WithContext(ctx)
			}
		}
		// ok=false (ambiguous multi-port or unknown portName) → ClusterIP URL unchanged
	}

	er.next.ServeHTTP(w, r)
}
