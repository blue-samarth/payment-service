package middleware

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestClientIP_From(t *testing.T) {
	trusted := NewClientIP([]netip.Prefix{
		netip.MustParsePrefix("10.1.0.0/16"),
		netip.MustParsePrefix("fd00::/8"),
	})

	cases := []struct {
		name     string
		resolver *ClientIP
		remote   string
		xff      []string
		want     string
	}{
		{"nil resolver ignores the header", nil, "203.0.113.5:1", []string{"198.51.100.7"}, "203.0.113.5"},
		{"no trusted proxies ignores the header", NewClientIP(nil), "203.0.113.5:1", []string{"198.51.100.7"}, "203.0.113.5"},
		{"untrusted peer ignores the header", trusted, "203.0.113.5:1", []string{"198.51.100.7"}, "203.0.113.5"},
		{"trusted peer without header", trusted, "10.1.2.3:1", nil, "10.1.2.3"},
		{"trusted peer uses the right-most hop", trusted, "10.1.2.3:1", []string{"198.51.100.7"}, "198.51.100.7"},
		{"caller-prepended hops are skipped", trusted, "10.1.2.3:1", []string{"1.1.1.1, 2.2.2.2, 198.51.100.7"}, "198.51.100.7"},
		{"trusted hops are walked past", trusted, "10.1.2.3:1", []string{"198.51.100.7, 10.1.9.9"}, "198.51.100.7"},
		{"repeated headers are one list", trusted, "10.1.2.3:1", []string{"1.1.1.1", "198.51.100.7, 10.1.9.9"}, "198.51.100.7"},
		{"unparseable hop falls back to the peer", trusted, "10.1.2.3:1", []string{"198.51.100.7, garbage"}, "10.1.2.3"},
		{"all hops trusted falls back to the peer", trusted, "10.1.2.3:1", []string{"10.1.4.4, 10.1.5.5"}, "10.1.2.3"},
		{"ipv6 peer", trusted, "[fd00::1]:1", []string{"2001:db8::7"}, "2001:db8::7"},
		{"ipv4-mapped peer is unmapped", trusted, "[::ffff:10.1.2.3]:1", []string{"198.51.100.7"}, "198.51.100.7"},
		{"remote addr without port", trusted, "203.0.113.5", nil, "203.0.113.5"},
		{"unparseable remote addr is returned as is", trusted, "pipe", []string{"198.51.100.7"}, "pipe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if got := tc.resolver.From(req); got != tc.want {
				t.Errorf("From() = %q, want %q", got, tc.want)
			}
		})
	}
}
