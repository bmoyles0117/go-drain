package drain

import (
	"errors"
	"net"
	"os"
	"os/signal"
	"sync"

	"github.com/pborman/uuid"
)

var (
	errNilListener = errors.New("Nil parent listener on the drain listener")
)

// ShutdownNotifier is the mechanism for informing your application that the
// listener is being shutdown, and to take action accordingly. It is very
// important that you close the ShutdownNotifier when your function returns.
type ShutdownNotifier struct {
	C chan interface{}

	id string
	l  *Listener

	closed bool
	mu     sync.Mutex
}

// Shutdown should ALWAYS be called when you're done with the shutdown notifier. The
// ShutdownNotifier is a leaky attribute of the listener if you do not eventually
// close the subscribed ShutdownNotifiers.
func (cn *ShutdownNotifier) Shutdown() error {
	cn.mu.Lock()
	if cn.closed {
		cn.mu.Unlock()
		return nil
	}

	cn.closed = true
	cn.mu.Unlock()

	if cn.l != nil {
		cn.l.mu.Lock()
		delete(cn.l.shutdownNotifiers, cn.id)
		cn.l.mu.Unlock()
	}

	close(cn.C)

	return nil
}

// A Listener wraps any net.Listener to support draining functionality at the
// connection level.
type Listener struct {
	parentListener    net.Listener
	closed            bool
	shutdown          bool
	wg                *sync.WaitGroup
	mu                sync.Mutex
	shutdownNotifiers map[string]*ShutdownNotifier
}

// Accept will produce errors if the parent listener is nil or if the listener
// has already been closed.
func (l *Listener) Accept() (net.Conn, error) {
	if l.parentListener == nil {
		return nil, errNilListener
	}

	if l.closed {
		return nil, errNilListener
	}

	parentConn, err := l.parentListener.Accept()
	if err != nil {
		return nil, err
	}

	return newConn(parentConn, l.wg)
}

// Addr will simply return the Addr of the parent listener.
func (l *Listener) Addr() net.Addr {
	if l.parentListener == nil {
		return nil
	}

	return l.parentListener.Addr()
}

// Close will inform every close notifier that is actively registered in the
// shutdownNotifiers map that the connection is going away. This helps inform
// any client interested in respecting the server's request to shutdown that
// immediate action should be taken to safely shut down.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}

	l.closed = true
	l.mu.Unlock()

	if l.parentListener != nil {
		l.parentListener.Close()
	}

	var closeErr error

	if l.wg != nil {
		l.wg.Wait()
	}

	if l.parentListener == nil {
		return nil
	}

	return closeErr
}

// NotifyShutdown will return a shutdownNotifier that can be used to subscribe to
// when the connection has been closed.
//
// It is VERY important that you defer Close on the close notifier that is
// returned to ensure that resources are properly. There is no strong binding
// between connections and close notifiers, so it is your responsibility to
// ensure that they are closed.
func (l *Listener) NotifyShutdown() *ShutdownNotifier {
	shutdownNotifier := &ShutdownNotifier{
		C:  make(chan interface{}),
		id: uuid.New(),
		l:  l,
	}

	if l.shutdown {
		shutdownNotifier.Shutdown()
	} else {
		l.mu.Lock()
		l.shutdownNotifiers[shutdownNotifier.id] = shutdownNotifier
		l.mu.Unlock()
	}

	return shutdownNotifier
}

// Shutdown is safe to call multiple times. Safe will inform the parent
// listener to close, which will propagate the Close command on this
// listener. It's a bit cyclic, and may have room for improvement, but this
// method is the preferred method for informing the listener to shutdown.
func (l *Listener) Shutdown() {
	l.mu.Lock()
	if l.shutdown {
		l.mu.Unlock()
		return
	}

	l.shutdown = true
	l.mu.Unlock()

	for id, shutdownNotifier := range l.shutdownNotifiers {
		shutdownNotifier.Shutdown()

		l.mu.Lock()
		delete(l.shutdownNotifiers, id)
		l.mu.Unlock()
	}

	if l.parentListener != nil {
		l.parentListener.Close()
	}
}

// ShutdownForKillSignals is just a convenience method to spawn off a goroutine
// that will catch any kill signals specified and trigger a shutdown.
func (l *Listener) ShutdownWhenSignalsNotified(signals ...os.Signal) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, signals...)

	go func() {
		select {
		case <-sigc:
			l.Shutdown()
		}
	}()
}

// Listen simply wraps a net.Listener as a Listener which also satisfies the
// net.Listener interface. Passing a nil parentListener will produce an error.
func Listen(parentListener net.Listener) (*Listener, error) {
	if parentListener == nil {
		return nil, errNilListener
	}

	return &Listener{
		parentListener:    parentListener,
		wg:                &sync.WaitGroup{},
		shutdownNotifiers: map[string]*ShutdownNotifier{},
	}, nil
}
