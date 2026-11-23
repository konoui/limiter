package main

import (
	"net/http"
	"testing"
)

func Test_remoteIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		want       string
		wantErr    bool
	}{
		{name: "net/http server ipv4", remoteAddr: "192.0.2.1:12345", want: "192.0.2.1"},
		{name: "net/http server ipv6", remoteAddr: "[2001:db8::1]:12345", want: "2001:db8::1"},
		{name: "lambda ipv4 without port", remoteAddr: "192.0.2.1", want: "192.0.2.1"},
		{name: "lambda ipv6 without port", remoteAddr: "2001:db8::1", want: "2001:db8::1"},
		{name: "empty", remoteAddr: "", wantErr: true},
		{name: "invalid", remoteAddr: "not-an-ip", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Request{RemoteAddr: tt.remoteAddr}
			got, err := remoteIP(r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("remoteIP() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("remoteIP() = %q, want %q", got, tt.want)
			}
		})
	}
}
