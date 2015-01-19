package main

import (
	"github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"net"
	"os"
)

var mh codec.MsgpackHandle

var log = logging.MustGetLogger("mphandler")
var format = "%{color}%{level} %{time:Jan 02 15:04:05} %{shortfile}%{color:reset} ▶ %{message}"
var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
var db *DB

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
			log.Debug("Handling incoming from %v", fromaddr)
			go handleIncoming(fromaddr.(*net.UDPAddr), buf[:n])
		}
	}
}

func getUint64(i interface{}) uint64 {
	switch i := i.(type) {
	case uint64:
		return i
	case int64:
		return uint64(i)
	default:
		return 0
	}
}

func handleIncoming(from *net.UDPAddr, buf []byte) {
	var (
		table      map[string]interface{}
		data       map[string]interface{}
		ret        map[string]interface{}
		conn       *net.UDPConn
		bucketname string
		keys       []string
		nodeid     uint64
		oper       string
		echo       uint64
		ok         bool
		found      bool
	)

	offset := 0
	newoffset, decoded := decode(&buf, offset)
	if table, ok = decoded.(map[string]interface{}); !ok {
		log.Debug("Did not decode msg %v as map[string]interface{}", decoded)
		return
	}

	if nodeid, found = table["nodeid"].(uint64); !found {
		log.Debug("Msg did not have key 'nodeid' (%v)", table)
		return
	}

	if oper, found = table["oper"].(string); !found {
		log.Debug("Msg did not have key 'oper' (%v)", table)
		return
	}

	if _echo, found := table["echo"]; !found {
		log.Debug("Msg did not have key 'echo' (%v)", table)
		return
	} else {
		echo = getUint64(_echo)
	}

	if data, found = table["data"].(map[string]interface{}); !found {
		log.Debug("Msg did not have key 'data' (%v)", table)
		return
	}

	log.Debug("Nodeid %v", nodeid)
	log.Debug("Oper %v", oper)
	log.Debug("Echo %v", echo)
	log.Debug("Data %v", data)

	var err error
	switch oper {
	case "PERSIST":
		err = db.Persist(string(nodeid), data)
	case "GETPERSIST":
		ret, err = db.GetPersist(string(nodeid), keys)
	case "INSERT":
		err = db.Insert(data)
	case "GET":
		ret, err = db.Get(keys)
	case "GETBUCKET":
		ret, err = db.GetBucket(bucketname)
	case "DELETE":
	case "SUBSCRIBE":
	default:
		log.Error("Unrecognized operation %v", oper)
	}

	if ret != nil {
		conn, err = net.DialUDP("udp6", nil, from)
		if err != nil {
			log.Error("could not create connection back to %v (%v)", from, err)
			return
		}
		buf := []byte{}
		packet := map[string]interface{}{
			"results": ret,
		}
		log.Debug("writing back %v", packet)
		encoder := codec.NewEncoderBytes(&buf, &mh)
		encoder.Encode(packet)
		_, err = conn.Write(buf)
		if err != nil {
			log.Error("Error writing to client %v (%v)", from, err)
		}
		if newoffset == len(buf) {
			log.Debug("finished decoding")
			return
		}
	}
}

func main() {
	mongoaddr, err := net.ResolveTCPAddr("tcp4", "0.0.0.0:27017")
	if err != nil {
		log.Fatal("Error parsing Mongo address: %v", err)
	}
	db = NewDB("mpdb.db")

	addr, err := net.ResolveUDPAddr("udp6", "[::]:7000")
	if err != nil {
		log.Error("Error resolving UDP address for msgpack %v", err)
	}
	ServeUDP(addr)
}
