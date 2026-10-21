package middleware

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

type ClientIP struct {
	trusted []netip.Prefix
}

func NewClientIP(trusted []netip.Prefix) *ClientIP {
	return &ClientIP{trusted: trusted}
}

func (c *ClientIP) From(r *http.Request) string {
	peer, ok := remoteAddr(r.RemoteAddr)
	if !ok {
		return r.RemoteAddr
	}
	if c == nil || !c.isTrusted(peer) {
		return peer.String()
	}
	hops := forwardedHops(r.Header.Values("X-Forwarded-For"))
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(hops[i])
		if err != nil {
			return peer.String()
		}
		hop = hop.Unmap()
		if !c.isTrusted(hop) {
			return hop.String()
		}
	}
	return peer.String()
}

func (c *ClientIP) isTrusted(addr netip.Addr) bool {
	for _, p := range c.trusted {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

func remoteAddr(raw string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(raw)
	if err != nil {
		host = raw
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func forwardedHops(headers []string) []string {
	var hops []string
	for _, h := range headers {
		for _, part := range strings.Split(h, ",") {
			if t := strings.TrimSpace(part); t != "" {
				hops = append(hops, t)
			}
		}
	}
	return hops
}
