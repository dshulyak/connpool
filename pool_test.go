package connpool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testConn struct {
	address int32
	tag     string
}

func (t *testConn) Close() error {
	return nil
}

func TestPoolRemote(t *testing.T) {
	dial := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	dialer := func(address int32) (Connection, error) {
		ready <- struct{}{}
		<-dial
		return &testConn{address: address, tag: "dialer"}, nil
	}
	p := New(WithDialer(dialer))
	received := make(chan Connection, 1)
	go func() {
		conn, _ := p.GetConnection(1)
		received <- conn
	}()
	<-ready
	expected := &testConn{address: 1, tag: "remote"}
	p.OnNewRemoteConnection(expected.address, expected)
	dial <- struct{}{}
	require.Equal(t, expected, <-received)
}

func TestPoolShutdown(t *testing.T) {
	dial := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	dialer := func(address int32) (Connection, error) {
		ready <- struct{}{}
		<-dial
		return &testConn{address: address, tag: "dialer"}, nil
	}
	p := New(WithDialer(dialer))
	received := make(chan error, 1)
	go func() {
		_, err := p.GetConnection(1)
		received <- err
	}()
	<-ready
	go func() {
		time.Sleep(100 * time.Millisecond) // this is flaky2
		dial <- struct{}{}
	}()
	p.Shutdown()

	require.ErrorIs(t, <-received, Closed)
}
