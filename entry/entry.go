//go:generate ffjson $GOFILE

package entry

import (
	"errors"
	"flag"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/sevein/nfdmp2rds/geoip"
)

var (
	// NoGeo lets the user decide if they want to use the geographic database.
	NoGeo = flag.Bool("nogeo", false, "Do not use geographic database")

	// Hostname is used in the JSON document.
	Hostname = flag.String("hostname", "localhost", "Given hostname")
)

// NfdumpEntry represents a nfdump entry
type NfdumpEntry struct {
	Host          string      `json:"host"`
	InBytes       string      `json:"in_bytes"`
	InPkts        string      `json:"in_pkts"`
	Ipv4SrcAddr   string      `json:"ipv4_src_addr"`
	Ipv4DstAddr   string      `json:"ipv4_dst_addr"`
	Protocol      string      `json:"protocol"`
	L4SrcPort     string      `json:"l4_src_port"`
	L4DstPort     string      `json:"l4_dst_port"`
	FirstSwitched string      `json:"first_switched"`
	LastSwitched  string      `json:"last_switched"`
	GeoIPSrc      *GeoIPEntry `json:"geoip_src,omitempty"`
	GeoIPDst      *GeoIPEntry `json:"geoip_dst,omitempty"`
}

// GeoIPEntry identifiers geographic location
type GeoIPEntry struct {
	IsoCode   string  `json:"iso_code,omitempty"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
}

const (
	delim         = "|"
	expectedParts = 24
)

// NewNfdumpEntry creates a new NfdumpEntry
func NewNfdumpEntry(s string) (*NfdumpEntry, error) {
	parts := strings.Split(s, delim)
	if len(parts) < expectedParts {
		return nil, errors.New("Unrecognized nfdump entry")
	}

	e := NfdumpEntry{
		Host:          *Hostname,
		InBytes:       parts[23],
		InPkts:        parts[22],
		Protocol:      parts[5],
		L4SrcPort:     parts[10],
		L4DstPort:     parts[15],
		FirstSwitched: ftime(parts[1]),
		LastSwitched:  ftime(parts[3]),
	}

	if !*NoGeo {
		ipv4Src, err := strlong2ip(parts[9])
		if err != nil {
			return nil, errors.New("Unrecognized IP address")
		}
		e.Ipv4SrcAddr = ipv4Src.String()

		if geo, err := geoip.Geo(ipv4Src); err == nil {
			e.GeoIPSrc = &GeoIPEntry{}
			if geo.Country.IsoCode != "" {
				e.GeoIPSrc.IsoCode = geo.Country.IsoCode
			}
			if geo.Location.Longitude != 0 {
				e.GeoIPSrc.Longitude = geo.Location.Longitude
			}
			if geo.Location.Latitude != 0 {
				e.GeoIPSrc.Latitude = geo.Location.Latitude
			}
		}

		ipv4Dst, err := strlong2ip(parts[14])
		if err != nil {
			return nil, errors.New("Unrecognized IP address")
		}
		e.Ipv4DstAddr = ipv4Dst.String()

		if geo, err := geoip.Geo(ipv4Dst); err == nil {
			e.GeoIPDst = &GeoIPEntry{}
			if geo.Country.IsoCode != "" {
				e.GeoIPDst.IsoCode = geo.Country.IsoCode
			}
			if geo.Location.Longitude != 0 {
				e.GeoIPDst.Longitude = geo.Location.Longitude
			}
			if geo.Location.Latitude != 0 {
				e.GeoIPDst.Latitude = geo.Location.Latitude
			}
		}
	}

	return &e, nil
}

func strlong2ip(s string) (net.IP, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return net.IPv4(byte(i>>24), byte(i>>16), byte(i>>8), byte(i)), nil
}

func ftime(s string) string {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ""
	}
	return time.Unix(i, 0).UTC().Format(time.RFC3339)
}
