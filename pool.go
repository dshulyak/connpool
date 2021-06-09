package connpool

import (
	"errors"
	"fmt"
	"sync"
)

const (
	opening status = iota + 1
	established
	failed
)

type status uint8

var statuses = [...]string{
	"opening",
	"established",
	"failed",
}

// Closed returned if pool was already closed.
var (
	Closed         = errors.New("connection pool is closed")
	ErrEstablished = errors.New("connection already exists")
)

func (c status) String() string {
	return statuses[c-1]
}

type Connection interface {
	Close() error
}

type Dialer func(int32) (Connection, error)

type state struct {
	conn Connection
	err  error
	wait chan struct{}
}

func (s *state) status() status {
	if s.err != nil {
		return failed
	}
	if s.conn == nil {
		return opening
	}
	return established
}

type Option func(*Pool)

func WithDialer(dialer Dialer) Option {
	return func(p *Pool) {
		p.dialer = dialer
	}
}

func New(opts ...Option) *Pool {
	p := &Pool{
		connections: map[int32]*state{},
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.dialer == nil {
		panic("require a Dialer instance")
	}
	return p
}

type Pool struct {
	dialer Dialer

	mu          sync.Mutex
	closed      bool
	opening     sync.WaitGroup
	connections map[int32]*state
}

func (p *Pool) GetConnection(address int32) (Connection, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, Closed
	}

	st, exist := p.connections[address]
	if !exist {
		st = &state{wait: make(chan struct{})}
		p.connections[address] = st
		p.opening.Add(1)
		go func() {
			defer p.opening.Done()
			conn, err := p.dialer(address)
			p.mu.Lock()
			defer p.mu.Unlock()
			// either pool was shutdown or we received concurrent onNewRemoteConnection
			if st.status() != opening {
				return
			}
			st.conn = conn
			st.err = err
			close(st.wait)
		}()
	}
	if st.status() == opening {
		p.mu.Unlock()
		<-st.wait
		p.mu.Lock()
		defer p.mu.Unlock()
		// call that initiated creation of the connection must cleanup after itself
		if st.err != nil && !exist {
			delete(p.connections, address)
		}
	} else {
		defer p.mu.Unlock()
	}
	return st.conn, st.err
}

func (p *Pool) OnNewRemoteConnection(address int32, conn Connection) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return Closed
	}
	st, exists := p.connections[address]
	// if another dial is in progress we will overwrite it, and it will gracefull exit
	// if connection already established return error that it already exists to possibly propagate it back
	// if failed overwrite failed connection

	if exists {
		if st.status() == opening {
			st.conn = conn
			close(st.wait)
		} else if st.status() == established {
			return fmt.Errorf("%w: address %d", ErrEstablished, address)
		} else if st.status() == failed {
			st.err = nil
			st.conn = conn
		}
	} else {
		p.connections[address] = &state{conn: conn}
	}
	return nil
}

func (p *Pool) Shutdown() {
	p.mu.Lock()
	p.closed = true
	for _, st := range p.connections {
		if st.status() == opening {
			st.err = Closed
			close(st.wait)
		}
	}
	p.mu.Unlock()
	p.opening.Wait()
}
