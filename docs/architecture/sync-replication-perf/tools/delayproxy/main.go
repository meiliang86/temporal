// delayproxy forwards TCP connections and delays every chunk of data, in each direction, by a fixed
// one-way delay, preserving order. It simulates network latency between two Temporal clusters.
//
//	delayproxy -listen 127.0.0.1:17233 -target 127.0.0.1:7233 -delay 20ms -control 127.0.0.1:17299
//
// The delay can be changed at runtime: curl '127.0.0.1:17299/delay?d=10ms'. It applies to data read
// after the change; open connections keep working.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var delay atomic.Int64

type chunk struct {
	data []byte
	due  time.Time
}

func pipe(dst, src net.Conn) {
	ch := make(chan chunk, 4096)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 64*1024)
			n, err := src.Read(buf)
			if n > 0 {
				ch <- chunk{data: buf[:n], due: time.Now().Add(time.Duration(delay.Load()))}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range ch {
		if wait := time.Until(c.due); wait > 0 {
			time.Sleep(wait)
		}
		if _, err := dst.Write(c.data); err != nil {
			break
		}
	}
	// Drain so the reader can exit, then close both sides.
	for range ch {
	}
	_ = dst.Close()
	_ = src.Close()
}

func main() {
	listen := flag.String("listen", "", "listen address")
	target := flag.String("target", "", "target address")
	d := flag.Duration("delay", 0, "one-way delay")
	control := flag.String("control", "", "HTTP control address (optional)")
	flag.Parse()
	delay.Store(int64(*d))

	if *control != "" {
		http.HandleFunc("/delay", func(w http.ResponseWriter, r *http.Request) {
			if v := r.URL.Query().Get("d"); v != "" {
				nd, err := time.ParseDuration(v)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				delay.Store(int64(nd))
			}
			_, _ = fmt.Fprintln(w, time.Duration(delay.Load()))
		})
		go func() { log.Fatal(http.ListenAndServe(*control, nil)) }()
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("delayproxy %s -> %s, one-way delay %v", *listen, *target, *d)
	for {
		in, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			out, err := net.Dial("tcp", *target)
			if err != nil {
				log.Printf("dial %s: %v", *target, err)
				_ = in.Close()
				return
			}
			go pipe(out, in)
			pipe(in, out)
		}()
	}
}

