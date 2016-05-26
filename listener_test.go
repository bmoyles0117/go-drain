package drain

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type mockAddr struct {
	network string
}

func (a *mockAddr) Network() string {
	return a.network
}

func (a *mockAddr) String() string {
	return a.network
}

type mockListener struct {
	acceptErr       error
	acceptedCounter int
	addrCounter     int
	closedCounter   int
	closeDelay      time.Duration
}

func (l *mockListener) Accept() (net.Conn, error) {
	l.acceptedCounter += 1

	if l.acceptErr != nil {
		return nil, l.acceptErr
	}

	return &mockConn{}, nil
}

func (l *mockListener) Addr() net.Addr { l.addrCounter += 1; return &mockAddr{"tcp"} }

func (l *mockListener) Close() error {
	time.Sleep(l.closeDelay)
	l.closedCounter += 1
	return nil
}

func TestListen(t *testing.T) {
	Convey("Listening to a nil listener should return an error", t, func() {
		drainListener, err := Listen(nil)
		So(drainListener, ShouldBeNil)
		So(err, ShouldResemble, errNilListener)
	})

	Convey("Listening to a valid listener should return a drain listener", t, func() {
		mockListener := &mockListener{}

		drainListener, err := Listen(mockListener)
		So(drainListener, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(drainListener.parentListener, ShouldPointTo, mockListener)
		So(drainListener.closed, ShouldBeFalse)
		So(drainListener.wg, ShouldNotBeNil)
		So(drainListener.shutdownNotifiers, ShouldNotBeNil)

		var _ net.Listener = drainListener
	})
}

func TestListenerAccept(t *testing.T) {
	Convey("Calling accept on a listener with a nil parent listener should return an error", t, func() {
		listener := &Listener{}
		c, err := listener.Accept()
		So(c, ShouldBeNil)
		So(err, ShouldResemble, errNilListener)
	})

	Convey("Calling accept on a closed listener should return an error", t, func() {
		listener := &Listener{
			parentListener: &mockListener{},
			closed:         true,
		}
		c, err := listener.Accept()
		So(c, ShouldBeNil)
		So(err, ShouldResemble, errNilListener)
	})

	Convey("Calling accept on a listener when the parent listener returns an error should propagage that error", t, func() {
		mockListener := &mockListener{
			acceptErr: errors.New("Some random accept error"),
		}

		listener := &Listener{
			parentListener: mockListener,
		}
		c, err := listener.Accept()
		So(c, ShouldBeNil)
		So(err, ShouldResemble, mockListener.acceptErr)
	})

	Convey("Calling accept on a listener with a populated parent listener should propagate the close call", t, func() {
		mockListener := &mockListener{}

		listener := &Listener{
			parentListener: mockListener,
			wg:             &sync.WaitGroup{},
		}

		So(mockListener.acceptedCounter, ShouldEqual, 0)
		c, err := listener.Accept()
		So(c, ShouldNotBeNil)
		So(err, ShouldBeNil)
		drainConn, ok := c.(*conn)
		// Ensure that we have wrapped the connection
		So(drainConn, ShouldNotBeNil)
		So(ok, ShouldBeTrue)

		// Make sure that we have properly assigned the connection's wait group
		So(drainConn.wg, ShouldPointTo, listener.wg)

		So(mockListener.acceptedCounter, ShouldEqual, 1)
		c, err = listener.Accept()
		So(c, ShouldNotBeNil)
		So(err, ShouldBeNil)
		drainConn, ok = c.(*conn)
		// Ensure that we have wrapped the connection
		So(drainConn, ShouldNotBeNil)
		So(ok, ShouldBeTrue)

		// Make sure that we have properly assigned the connection's wait group
		So(drainConn.wg, ShouldPointTo, listener.wg)

		So(mockListener.acceptedCounter, ShouldEqual, 2)
	})
}

func TestListenerAddr(t *testing.T) {
	Convey("Calling addr on a listener with a nil parent listener should be safe and return nil", t, func() {
		listener := &Listener{}

		addr := listener.Addr()
		So(addr, ShouldBeNil)
	})

	Convey("Calling addr on a listener with a populated parent listener should call return the parent listener's addr", t, func() {
		mockListener := &mockListener{}

		listener := &Listener{
			parentListener: mockListener,
		}

		So(mockListener.addrCounter, ShouldEqual, 0)
		expectedAddr := mockListener.Addr()
		So(mockListener.addrCounter, ShouldEqual, 1)

		So(mockListener.addrCounter, ShouldEqual, 1)
		addr := listener.Addr()
		So(addr, ShouldNotBeNil)
		So(addr, ShouldResemble, expectedAddr)
		So(mockListener.addrCounter, ShouldEqual, 2)

		So(mockListener.addrCounter, ShouldEqual, 2)
		addr = listener.Addr()
		So(addr, ShouldNotBeNil)
		So(addr, ShouldResemble, expectedAddr)
		So(mockListener.addrCounter, ShouldEqual, 3)
	})
}

func TestListenerClose(t *testing.T) {
	Convey("Calling close on a listener with a nil parent listener should be safe and set the state to closed", t, func() {
		listener := &Listener{}
		So(listener.closed, ShouldBeFalse)

		closeErr := listener.Close()
		So(closeErr, ShouldBeNil)
		So(listener.closed, ShouldBeTrue)
	})

	Convey("Calling close on a listener with a populated parent listener should propagate the close call at most once", t, func() {
		mockListener := &mockListener{}

		listener := &Listener{
			parentListener: mockListener,
		}

		So(mockListener.closedCounter, ShouldEqual, 0)
		closeErr := listener.Close()
		So(closeErr, ShouldBeNil)
		So(mockListener.closedCounter, ShouldEqual, 1)

		So(mockListener.closedCounter, ShouldEqual, 1)
		closeErr = listener.Close()
		So(closeErr, ShouldBeNil)
		So(mockListener.closedCounter, ShouldEqual, 1)

		So(mockListener.closedCounter, ShouldEqual, 1)
		closeErr = listener.Close()
		So(closeErr, ShouldBeNil)
		So(mockListener.closedCounter, ShouldEqual, 1)
	})

	Convey("Calling close on a listener with a populated wait group should block on the wait group", t, func() {
		mockListener := &mockListener{}

		wg := &sync.WaitGroup{}
		wg.Add(1)

		listener := &Listener{
			parentListener: mockListener,
			wg:             wg,
		}

		listenerClosed := make(chan bool)

		go func() {
			listener.Close()

			listenerClosed <- true
		}()

		select {
		case <-listenerClosed:
			t.Error("We should not have been able to close the listener due to a populated wait group")
		case <-time.After(10 * time.Millisecond):
			// Good to go!
		}
	})

	Convey("Closing the listener concurrently should only emit one Close to the parent listener", t, func() {
		mockListener := &mockListener{
			closeDelay: 10 * time.Millisecond,
		}

		listener := &Listener{
			parentListener: mockListener,
			wg:             &sync.WaitGroup{},
		}

		So(mockListener.closedCounter, ShouldEqual, 0)

		go listener.Close()
		go listener.Close()

		time.Sleep(20 * time.Millisecond)

		So(mockListener.closedCounter, ShouldEqual, 1)
	})
}

func TestListenerNotifyShutdown(t *testing.T) {
	Convey("Notify close should never produce a message on the chan if the listener is never closed", t, func() {
		mockListener := &mockListener{}

		listener, err := Listen(mockListener)
		So(listener, ShouldNotBeNil)
		So(err, ShouldBeNil)

		closeNotifier := listener.NotifyShutdown()

		select {
		case <-closeNotifier.C:
			t.Error("We should not have received a close signal")
		default:
			// Good to go!
		}
	})

	Convey("Notify close should produce a message when the listener is shutdown", t, func() {
		mockListener := &mockListener{}

		listener, err := Listen(mockListener)
		So(listener, ShouldNotBeNil)
		So(err, ShouldBeNil)

		beforeShutdownNotifier := listener.NotifyShutdown()

		listener.Shutdown()

		select {
		case <-beforeShutdownNotifier.C:
			// Good to go!
		case <-time.After(10 * time.Millisecond):
			t.Error("Failed to be notified that the listener has been shutdown!")
		}

		afterShutdownNotifier := listener.NotifyShutdown()

		select {
		case <-afterShutdownNotifier.C:
			// Good to go!
		case <-time.After(10 * time.Millisecond):
			t.Error("Failed to be notified that the listener has been shutdown!")
		}
	})
}

func TestListenerShutdown(t *testing.T) {
	Convey("Shutdown on a listener without a parent should be safe", t, func() {
		listener := &Listener{}

		So(func() { listener.Shutdown() }, ShouldNotPanic)
	})

	Convey("Shutdown on a listener with a parent should propagate the close call", t, func() {
		mockListener := &mockListener{}

		listener := &Listener{
			parentListener: mockListener,
		}

		So(mockListener.closedCounter, ShouldEqual, 0)
		So(func() { listener.Shutdown() }, ShouldNotPanic)
		So(mockListener.closedCounter, ShouldEqual, 1)

		So(mockListener.closedCounter, ShouldEqual, 1)
		So(func() { listener.Shutdown() }, ShouldNotPanic)
		So(mockListener.closedCounter, ShouldEqual, 1)
	})
}
