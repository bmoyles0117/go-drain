package drain

import (
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type mockConn struct {
	closedCounter int
}

func (c *mockConn) Close() error {
	c.closedCounter += 1

	return nil
}

func (c *mockConn) LocalAddr() net.Addr                { return nil }
func (c *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (c *mockConn) RemoteAddr() net.Addr               { return nil }
func (c *mockConn) SetDeadline(t time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *mockConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *mockConn) Write(b []byte) (int, error)        { return 0, nil }

func writeToChanIfWaitGroupDone(wg *sync.WaitGroup) chan bool {
	wgDone := make(chan bool)

	go func() {
		wg.Wait()
		wgDone <- true
	}()

	return wgDone
}

func TestNewConn(t *testing.T) {
	Convey("Creating a conn with a nil conn should return an error", t, func() {
		conn, err := newConn(nil, nil)
		So(conn, ShouldBeNil)
		So(err, ShouldEqual, errNilConnProvidedToConn)
	})

	Convey("Creating a conn with a nil wait group should return an error", t, func() {
		parentConn := &mockConn{}

		conn, err := newConn(parentConn, nil)
		So(conn, ShouldBeNil)
		So(err, ShouldEqual, errNilWaitGroupProvidedToConn)
	})

	Convey("Creating a new conn and waiting for the wait group should fail if we don't close the conn", t, func() {
		parentConn := &mockConn{}
		wg := &sync.WaitGroup{}

		conn, err := newConn(parentConn, wg)
		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)

		// conn should be a net.Conn
		var _ net.Conn = conn

		select {
		case <-time.After(10 * time.Millisecond):
			// Good to go!

		case <-writeToChanIfWaitGroupDone(wg):
			t.Error("We should not have been return from wg.Wait() due to not closing the conn")
		}
	})

	Convey("Creating a new conn and waiting for the wait group should succeed if we close the conn", t, func() {
		parentConn := &mockConn{}
		wg := &sync.WaitGroup{}

		conn, err := newConn(parentConn, wg)
		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)

		// conn should be a net.Conn
		var _ net.Conn = conn

		// We should not receive an error while closing the connection
		So(conn.Close(), ShouldBeNil)

		select {
		case <-time.After(10 * time.Millisecond):
			t.Error("The conn has been closed, the wait group should have been decremented")

		case <-writeToChanIfWaitGroupDone(wg):
			// Good to go!
		}
	})
}

func TestConnClose(t *testing.T) {
	Convey("Closing an conn with nil fields should be safe", t, func() {
		c := &conn{}
		So(c.Close(), ShouldBeNil)
	})

	Convey("Closing a conn with a wait group that has not been incremented should be safe", t, func() {
		wg := &sync.WaitGroup{}

		c := &conn{
			wg: wg,
		}

		So(c.Close(), ShouldBeNil)
	})

	Convey("Closing a conn should close the parent conn", t, func() {
		mockConn := &mockConn{}
		wg := &sync.WaitGroup{}

		c := &conn{
			parentConn: mockConn,
			wg:         wg,
		}

		So(mockConn.closedCounter, ShouldEqual, 0)
		So(c.Close(), ShouldBeNil)
		So(mockConn.closedCounter, ShouldEqual, 1)
	})

	Convey("Closing a conn multiple times should only close the parent conn once, and return an error the second time", t, func() {
		mockConn := &mockConn{}
		wg := &sync.WaitGroup{}

		c := &conn{
			parentConn: mockConn,
			wg:         wg,
		}

		So(c.closed, ShouldBeFalse)
		So(mockConn.closedCounter, ShouldEqual, 0)

		So(c.Close(), ShouldBeNil)
		So(mockConn.closedCounter, ShouldEqual, 1)
		So(c.closed, ShouldBeTrue)

		So(c.Close(), ShouldResemble, errConnClosed)
		So(mockConn.closedCounter, ShouldEqual, 1)
		So(c.closed, ShouldBeTrue)
	})
}
