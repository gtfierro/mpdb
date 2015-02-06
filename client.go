package main

import (
	"fmt"
	"github.com/ugorji/go/codec"
	"net"
	"time"
)

//var mh codec.MsgpackHandle

type Client struct {
	// address of the client
	addr *net.UDPAddr
	// Node ID
	nodeid uint64
	// the last echo tag we processed
	lastEcho uint64
	// key-value = echo:message for echo tags we can't commit yet
	cached map[uint64]map[string]interface{}
	// processing queue of messages
	queue chan map[string]interface{}
	// server time-out
	timeout time.Duration
	timer   <-chan time.Time
}

func NewClient(timeout time.Duration, addr *net.UDPAddr) *Client {
	address_nodeid := uint64(addr.IP[12])<<12 | uint64(addr.IP[13])<<8 | uint64(addr.IP[14])<<4 | uint64(addr.IP[15])
	log.Debug("string %v nodeid %v", addr.String(), address_nodeid)
	c := &Client{nodeid: address_nodeid, timeout: timeout, addr: addr, lastEcho: 0, cached: make(map[uint64]map[string]interface{}), queue: make(chan map[string]interface{})}
	go c.loop()
	return c
}

func (c *Client) loop() {
	for {
		select {
		case msg := <-c.queue:
			log.Debug("in queue")
			c.commitAndReply(msg)
		case <-c.timer:
			// handle next largest cached echo tag
			log.Debug("handle out of order")
			tmpecho := c.lastEcho
			for {
				tmpecho += 1
				if msg, found := c.cached[tmpecho]; found {
					log.Debug("found and executing %v for tag %v", tmpecho, msg)
					c.commitAndReply(msg)
				} else {
					if int(tmpecho-c.lastEcho) > 10 {
						break
					}
				}
			}
		}
	}
}

func (c *Client) handleIncoming(buf []byte, writeback *net.UDPConn) {
	var (
		msg  map[string]interface{}
		echo uint64
		ok   bool
	)

	offset := 0
	_, decoded := decode(&buf, offset) // decode msgpack
	log.Debug("client w/ addr %v decoded %v", c.addr, decoded)

	// decode top-level msg
	if msg, ok = decoded.(map[string]interface{}); !ok {
		log.Debug("Did not decode msg %v as map[string]interface{}", decoded)
		return
	}

	// get echo tag
	if _echo, found := msg["echo"]; !found {
		log.Debug("Msg did not have key 'echo' (%v)", msg)
		return
	} else {
		echo = getUint64(_echo)
	}

	// check echo tag
	switch {
	case echo < c.lastEcho: // an old echo tag that just got here
		log.Debug("old echo %v LE %v", echo, c.lastEcho)
		c.queue <- msg
	case echo == c.lastEcho+1: // the next message we want to handle
		log.Debug("handling echo %v LE %v", echo, c.lastEcho)
		c.queue <- msg
	case echo > c.lastEcho+1: // too far in the future, start timeout
		log.Debug("future echo %v LE %v", echo, c.lastEcho)
		c.cached[echo] = msg
		c.timer = time.After(c.timeout)
	case echo == c.lastEcho:
		log.Debug("duplicate echo %v LE %v", echo, c.lastEcho)
		c.queue <- msg
	default: // duplicate! ignore
		// why are you here
	}
}

func (c *Client) commitAndReply(msg map[string]interface{}) {
	var (
		data       map[string]interface{}
		keys       []string
		bucketname string
		nodeid     uint64
		echo       uint64
		oper       string
		ok         bool
	)

	// retrieve nodeid
	if _nodeid, found := msg["nodeid"]; !found {
		log.Debug("Msg did not have key 'nodeid' (%v)", msg)
		return
	} else {
		nodeid = getUint64(_nodeid)
	}

	// retrieve operation
	if _oper, found := msg["oper"]; !found {
		log.Debug("Msg did not have key 'oper' (%v)", msg)
		return
	} else {
		oper = _oper.(string)
	}

	echo = getUint64(msg["echo"])

	ok = true
	var (
		err error
		ret map[string]interface{}
	)
	log.Debug("handling %v", oper)
	switch oper {
	case "PERSIST":
		if nodeid != c.nodeid {
			err = fmt.Errorf("Node %v cannot access data with nodeid %v", c.nodeid, nodeid)
		} else {
			err = db.Persist(string(nodeid), data)
		}
	case "GETPERSIST":
		if nodeid != c.nodeid {
			err = fmt.Errorf("Node %v cannot access data with nodeid %v", c.nodeid, nodeid)
		} else {
			ret, err = db.GetPersist(string(nodeid), keys)
		}
	case "INSERT":
		err = db.Insert(data)
	case "GET":
		ret, err = db.Get(keys)
	case "GETBUCKET":
		ret, err = db.GetBucket(bucketname)
	case "DELETE":
		fallthrough
	case "SUBSCRIBE":
		fallthrough
	default:
		ok = false
		log.Error("Unrecognized operation %v", oper)
	}

	// delete entry in cache if it exists
	log.Debug("ok? %v %v %v", ok, echo, c.lastEcho)
	if ok {
		c.lastEcho = echo
		if _, found := c.cached[echo]; found {
			delete(c.cached, echo)
		}
	}

	// create message to send back
	packet := map[string]interface{}{
		"oper":   "RESPONSE",
		"nodeid": nodeid,
		"echo":   echo,
		"result": ret,
		"acks":   []uint64{},
	}
	if err != nil {
		packet["error"] = err.Error()
	} else {
		packet["err"] = nil
	}

	// dial back client
	conn, err := net.DialUDP("udp6", nil, c.addr)
	if err != nil {
		log.Error("could not create connection back to %v (%v)", c.addr, err)
		return
	}
	// and write message
	buf := []byte{}
	log.Debug("writing back %v", packet)
	encoder := codec.NewEncoderBytes(&buf, &mh)
	encoder.Encode(packet)
	_, err = conn.Write(buf)
	if err != nil {
		log.Error("Error writing to client %v (%v)", c.addr, err)
	}

	// now check for any messages we can now handle
	tmpecho := c.lastEcho
	for {
		tmpecho += 1
		if msg, found := c.cached[tmpecho]; found {
			log.Debug("found and executing %v for tag %v", tmpecho, msg)
			c.commitAndReply(msg)
		} else {
			if int(tmpecho-c.lastEcho) > 10 {
				break
			}
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
