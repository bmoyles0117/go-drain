package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/bmoyles0117/go-drain"
)

func main() {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	drainListener, err := drain.Listen(l)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	drainListener.ShutdownWhenSignalsNotified(syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	http.Serve(drainListener, indexHandler(drainListener))
}

func indexHandler(drainListener *drain.Listener) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		shutdownNotifier := drainListener.NotifyShutdown()
		defer shutdownNotifier.Shutdown()

		loop := true

		for i := 0; loop && i < 5; i++ {
			time.Sleep(1 * time.Second)
			fmt.Fprintf(w, "Hit %d second mark\n", i+1)

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}

			if r.FormValue("force_wait") != "1" {
				select {
				case <-shutdownNotifier.C:
					loop = false
				default:
				}
			}
		}

		fmt.Fprint(w, "Done")
	})
}
