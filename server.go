// License stuff

// Package mpdb is MsgPack Database -- a collection-based key-value store for
// embedded clients
package main

import (
	"github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"net"
	"os"
	"time"
)

var mh codec.MsgpackHandle

var log = logging.MustGetLogger("mphandler")
var format = "%{color}%{level} %{time:Jan 02 15:04:05} %{shortfile}%{color:reset} ▶ %{message}"
var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
var db *DB
var clients = make(map[string]*Client)

func ServeUDP(addr *net.UDPAddr) {
	conn, err := net.ListenUDP("udp6", addr)
	if err != nil {
		log.Error("Error on listening: %v", err)
	}
	defer conn.Close()

	for {
		buf := make([]byte, 4096)
		n, fromaddr, err := conn.ReadFrom(buf)
		if err != nil {
			log.Error("Problem reading connection %v", err)
		}
		if n > 0 {
			var client *Client
			var found bool
			var addr *net.UDPAddr = fromaddr.(*net.UDPAddr)
			log.Debug("Handling incoming from %v", addr)
			if client, found = clients[addr.String()]; !found {
				log.Debug("creating new client")
				client = NewClient(3*time.Second, addr)
				clients[addr.String()] = client
			}
			client.handleIncoming(buf[:n], nil)
		}
	}
}

func main() {
	db = NewDB("mpdb.db")

	addr, err := net.ResolveUDPAddr("udp6", "[::]:7000")
	if err != nil {
		log.Error("Error resolving UDP address for msgpack %v", err)
	}
	ServeUDP(addr)
}
