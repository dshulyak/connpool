package connpool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
		time.Sleep(time.Millisecond) // this is flaky
		dial <- struct{}{}
	}()
	p.Shutdown()

	require.ErrorIs(t, <-received, Closed)
}

func TestPoolConcurrentDial(t *testing.T) {
	var cnt uint64
	dialer := func(address int32) (Connection, error) {
		return &testConn{address: address, tag: fmt.Sprintf("%d", atomic.AddUint64(&cnt, 1))}, nil
	}
	p := New(WithDialer(dialer))

	var wg sync.WaitGroup
	n := 10
	wg.Add(n)

	received := make(chan Connection, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			conn, _ := p.GetConnection(1)
			received <- conn
		}()
	}
	wg.Wait()
	close(received)
	for conn := range received {
		require.Equal(t, &testConn{address: 1, tag: "1"}, conn)
	}
}

func TestConnectAfterError(t *testing.T) {
	var expectErr error
	dialer := func(address int32) (Connection, error) {
		if expectErr != nil {
			return nil, expectErr
		}
		return &testConn{address: address, tag: "dialer"}, nil
	}
	p := New(WithDialer(dialer))
	expectErr = errors.New("test")
	conn, err := p.GetConnection(1)
	require.Nil(t, conn)
	require.ErrorIs(t, err, expectErr)

	expectErr = nil
	conn, err = p.GetConnection(1)
	require.NotNil(t, conn)
	require.NoError(t, err)
}

func TestDifferentNotBlocked(t *testing.T) {
	long := int32(10)
	dial := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	dialer := func(address int32) (Connection, error) {
		if address == long {
			ready <- struct{}{}
			<-dial
		}
		return &testConn{address: address}, nil
	}
	p := New(WithDialer(dialer))
	received := make(chan Connection, 1)
	go func() {
		conn, _ := p.GetConnection(long)
		received <- conn
	}()
	<-ready
	conn, _ := p.GetConnection(1)
	require.Equal(t, conn, &testConn{address: 1})
	dial <- struct{}{}
	require.Equal(t, <-received, &testConn{address: long})
}

func BenchmarkGetConnection(b *testing.B) {
	dialer := func(address int32) (Connection, error) {
		return &testConn{address: address}, nil
	}
	p := New(WithDialer(dialer))
	b.RunParallel(func(pb *testing.PB) {
		i := int32(0)
		for pb.Next() {
			p.GetConnection(i)
			i++
		}
	})
}
