package entry

import "testing"

func TestMarshal(t *testing.T) {
	var tests = []struct {
		host  string
		input string
		expec string
	}{
		{
			"localhost",
			"2|1463425844|692|1463425855|188|6|0|0|0|2386192149|443|0|0|0|3641448481|57145|64512|12357|39|41|0|0|10|5256",
			`{ "host":"localhost","in_bytes":"5256","in_pkts":"10","ipv4_src_addr":"142.58.103.21","ipv4_dst_addr":"217.12.24.33","protocol":"6","l4_src_port":"443","l4_dst_port":"57145","first_switched":"2016-05-16T19:10:44Z","last_switched":"2016-05-16T19:10:55Z","geoip_src":{ "iso_code":"CA"},"geoip_dst":{ "iso_code":"ES"}}`,
		},
		{
			"different.hostname.tld",
			"2|1463425829|17|1463425834|5|6|0|0|0|2386192149|179|0|0|0|3641448481|11482|0|0|39|0|24|0|2|99",
			`{ "host":"different.hostname.tld","in_bytes":"99","in_pkts":"2","ipv4_src_addr":"142.58.103.21","ipv4_dst_addr":"217.12.24.33","protocol":"6","l4_src_port":"179","l4_dst_port":"11482","first_switched":"2016-05-16T19:10:29Z","last_switched":"2016-05-16T19:10:34Z","geoip_src":{ "iso_code":"CA"},"geoip_dst":{ "iso_code":"ES"}}`,
		},
	}
	for _, tt := range tests {
		Hostname = tt.host
		entry, _ := NewNfdumpEntry(tt.input)
		actual, _ := entry.MarshalJSON()
		if string(actual) != tt.expec {
			t.Errorf("marshal: expected %s, actual %s", tt.expec, actual)
		}
	}
}

func TestIp(t *testing.T) {
	var tests = []struct {
		input string
		expec string
	}{
		{"3232235777", "192.168.1.1"},
		{"3627729233", "216.58.193.81"},
	}
	for _, tt := range tests {
		actual, err := strlong2ip(tt.input)
		if err != nil {
			t.Error(err)
		}
		ipv4 := actual.String()
		if ipv4 != tt.expec {
			t.Errorf("ip(%s): expected %s, actual %s", tt.input, tt.expec, ipv4)
		}
	}
}

func TestFtime(t *testing.T) {
	var tests = []struct {
		input string
		expec string
	}{
		{"1463958401", "2016-05-22T23:06:41Z"},
		{"1234567890", "2009-02-13T23:31:30Z"},
	}
	for _, tt := range tests {
		actual := ftime(tt.input)
		if actual != tt.expec {
			t.Errorf("finput(%s): expected %s, actual %s", tt.input, tt.expec, actual)
		}
	}
}
