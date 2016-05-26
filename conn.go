package drain

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

var (
	errConnClosed                 = errors.New("conn has already been closed")
	errNilConnProvidedToConn      = errors.New("nil parent conn provided to conn")
	errNilWaitGroupProvidedToConn = errors.New("nil wait group provided to conn")
)

// conn is an abstraction on any net.Conn that simply deducts from the
// associated wait group when the connection is closed.
type conn struct {
	parentConn net.Conn
	closed     bool
	wg         *sync.WaitGroup
}

// Close will set the local state to closed, and close the parent net.Conn as
// well as attempt to deduct from the associated wait group.
func (c *conn) Close() error {
	if c.closed {
		return errConnClosed
	}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panicked while closing conn: %s", r)

			return
		}
	}()

	c.closed = true

	if c.parentConn != nil {
		c.parentConn.Close()
	}

	if c.wg != nil {
		c.wg.Done()
	}

	return nil
}

func (c *conn) LocalAddr() net.Addr                { return c.parentConn.LocalAddr() }
func (c *conn) Read(b []byte) (int, error)         { return c.parentConn.Read(b) }
func (c *conn) RemoteAddr() net.Addr               { return c.parentConn.RemoteAddr() }
func (c *conn) SetDeadline(t time.Time) error      { return c.parentConn.SetDeadline(t) }
func (c *conn) SetReadDeadline(t time.Time) error  { return c.parentConn.SetReadDeadline(t) }
func (c *conn) SetWriteDeadline(t time.Time) error { return c.parentConn.SetWriteDeadline(t) }
func (c *conn) Write(b []byte) (int, error)        { return c.parentConn.Write(b) }

func newConn(parentConn net.Conn, wg *sync.WaitGroup) (*conn, error) {
	if parentConn == nil {
		return nil, errNilConnProvidedToConn
	}

	if wg == nil {
		return nil, errNilWaitGroupProvidedToConn
	}

	wg.Add(1)

	return &conn{
		parentConn: parentConn,
		closed:     false,
		wg:         wg,
	}, nil
}
