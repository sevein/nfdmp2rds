//go:generate ffjson $GOFILE

package entry

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

// NfdumpEntry represents a nfdump entry
type NfdumpEntry struct {
	Host          string `json:"host"`
	InBytes       string `json:"in_bytes"`
	InPkts        string `json:"in_pkts"`
	Ipv4SrcAddr   string `json:"ipv4_src_addr"`
	Ipv4DstAddr   string `json:"ipv4_dst_addr"`
	Protocol      string `json:"protocol"`
	L4SrcPort     string `json:"l4_src_port"`
	L4DstPort     string `json:"l4_dst_port"`
	FirstSwitched string `json:"first_switched"`
	LastSwitched  string `json:"last_switched"`
}

const (
	delim         = "|"
	expectedParts = 24
)

// Host is exported so it can be customized
var Host = "localhost"

// NewNfdumpEntry creates a new NfdumpEntry
func NewNfdumpEntry(s string) (*NfdumpEntry, error) {
	parts := strings.Split(s, delim)
	if len(parts) < expectedParts {
		return nil, errors.New("Unrecognized nfdump entry")
	}
	e := NfdumpEntry{
		Host:          Host,
		InBytes:       parts[23],
		InPkts:        parts[22],
		Ipv4SrcAddr:   ip(parts[9]),
		Ipv4DstAddr:   ip(parts[14]),
		Protocol:      parts[5],
		L4SrcPort:     parts[10],
		L4DstPort:     parts[15],
		FirstSwitched: ftime(parts[1]),
		LastSwitched:  ftime(parts[3]),
	}
	return &e, nil
}

// ip converts the 32-bit integer representation of the IP address into its
// dotted-decimal notation.
func ip(s string) string {
	if i, err := strconv.ParseInt(s, 10, 32); err == nil {
		return net.IPv4(
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i)).String()
	}
	return ""
}

func ftime(s string) string {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ""
	}
	return time.Unix(i, 0).Format(time.RFC3339)
}
