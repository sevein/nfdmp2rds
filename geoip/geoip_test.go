package geoip_test

import (
	"net"
	"testing"

	"github.com/sevein/nfdmp2rds/geoip"
)

func TestLookup(t *testing.T) {
	var tests = []struct {
		input string
		expec string
	}{
		{"142.58.103.21", "CA"},
		{"217.12.24.33", "ES"},
	}
	for _, tt := range tests {
		ip := net.ParseIP(tt.input)
		r, err := geoip.Geo(ip)
		if err != nil {
			t.Error(err)
		}
		actual := r.Country.IsoCode
		if actual != tt.expec {
			t.Errorf("lookup: expected %s, actual %s", tt.expec, actual)
		}
	}
}
