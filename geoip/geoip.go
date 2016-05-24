//go:generate go-bindata -pkg $GOPACKAGE -o assets.go ../data/GeoLite2-Country.mmdb

package geoip

import (
	"fmt"
	"net"
	"time"

	maxmind "github.com/oschwald/maxminddb-golang"
)

var reader *maxmind.Reader

func init() {
	var err error
	reader, err = maxmind.FromBytes(MustAsset("../data/GeoLite2-Country.mmdb"))
	if err != nil {
		panic(err)
	}
}

// Geodata is a struct with the geographic data that we need from the GeoLite2
// database.
type Geodata struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

// Geo returns the Geodata of a given IP address.
func Geo(ip net.IP) (*Geodata, error) {
	var record Geodata
	err := reader.Lookup(ip, &record)
	return &record, err
}

// Info returns a string with information about the current database.
func Info() string {
	m := reader.Metadata

	return fmt.Sprintf("%s %d.%d (%s)",
		m.Description["en"],
		m.BinaryFormatMajorVersion,
		m.BinaryFormatMinorVersion,
		time.Unix(int64(m.BuildEpoch), 0).UTC().Format(time.RFC822))
}
