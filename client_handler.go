package main

import (
	"fmt"
	"github.com/ugorji/go/codec"
	"net"
	"time"
)

type Client struct {
	// address of the client
	addr *net.UDPAddr
	// Node ID
	nodeid uint64
	// window start. We have ACK'd all messages up until this echo tag
	window uint64
	// window size
	windowSize uint64
	// the last echo tag we have committed. Should be within the window
	lastCommitted uint64
	// key-value = echo:message for echo tags we can't commit yet
	cached map[uint64]map[string]interface{}
	// cache of responses for resends
	cachedResp  map[uint64]map[string]interface{}
	resendTimer *time.Ticker
	// processing queue of messages
	queue chan map[string]interface{}
	// server time-out
	timeout     time.Duration
	servertimer <-chan time.Time
}

func NewClient(timeout time.Duration, addr *net.UDPAddr) *Client {
	address_nodeid := uint64(addr.IP[12])<<12 | uint64(addr.IP[13])<<8 | uint64(addr.IP[14])<<4 | uint64(addr.IP[15])
	log.Debug("string %v nodeid %v", addr.String(), address_nodeid)
	c := &Client{nodeid: address_nodeid, timeout: timeout,
		addr: addr, window: 1, windowSize: 5, lastCommitted: 0,
		cached:      make(map[uint64]map[string]interface{}),
		cachedResp:  make(map[uint64]map[string]interface{}),
		resendTimer: time.NewTicker(timeout),
		queue:       make(chan map[string]interface{})}
	go c.loop()
	return c
}

func (c *Client) loop() {
	for {
		select {
		case msg := <-c.queue:
			log.Debug("got msg off queue")
			// get echo tag from message
			echo := getUint64(msg["echo"])
			if echo == c.lastCommitted+1 { // next in line to be processed
				log.Debug("commit %v -- after last committed %v", echo, c.lastCommitted)
				c.commitAndReply(msg)
			}
		case <-c.resendTimer.C:
			log.Debug("resending committed messages in window %v til %v", c.window, c.lastCommitted)
			for echo := c.window; echo <= c.lastCommitted; echo++ {
				c.doSend(c.cachedResp[echo])
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
	// this is a duplicate message, so we resend? TODO
	case echo < c.window:
		log.Debug("received duplicate Echo %v. Window starts at %v", echo, c.window)
	// within the window, so we queue to process
	case echo >= c.window && echo < c.window+c.windowSize:
		log.Debug("Received echo %v within window starting at %v", echo, c.window)
		c.cached[echo] = msg // cache the message
		c.queue <- msg       // queue to send
	// beyond the window and we've alrady processed it on this side. Check if we can
	// update the window
	case echo >= c.window+c.windowSize:
		diff := echo - (c.window + c.windowSize - 1)
		if diff <= (c.lastCommitted - c.window + 1) { // advance window by diff
			c.window += diff
			c.cached[echo] = msg
			c.queue <- msg
			// throw out ACK'd responses below our window
			for prevecho, _ := range c.cachedResp {
				if prevecho < c.window {
					delete(c.cachedResp, prevecho)
				}
			}
			log.Debug("advanced window by %v to %v", diff, c.window)
		}
		log.Debug("Received echo %v outside of window starting at %v", echo, c.window)
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
	log.Debug("COMMIT oper %v echo %v", oper, echo)
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

	// delete entry in cache if it exists and update state variables
	if ok {
		c.lastCommitted = echo
		if _, found := c.cached[echo]; found {
			delete(c.cached, echo)
		}
	}

	// create response
	packet := map[string]interface{}{
		"oper":   "RESPONSE",
		"nodeid": nodeid,
		"echo":   echo,
		"result": ret,
	}
	if err != nil {
		packet["error"] = err.Error()
	} else {
		packet["err"] = nil
	}

	c.doSend(packet)

	// cache the response
	c.cachedResp[echo] = packet

	// check for new messages we can process
	tmpecho := c.lastCommitted
	for {
		tmpecho += 1
		if msg, found := c.cached[tmpecho]; found {
			log.Debug("found and executing %v for tag %v", tmpecho, msg)
			if tmpecho == c.lastCommitted+1 { // next in line to be processed
				c.commitAndReply(msg)
			}
		} else if tmpecho > c.window+c.windowSize {
			break
		}
	}

}

func (c *Client) doSend(msg map[string]interface{}) {
	// dial back client
	conn, err := net.DialUDP("udp6", nil, c.addr)
	if err != nil {
		log.Error("could not create connection back to %v (%v)", c.addr, err)
		return
	}
	// and write message
	buf := []byte{}
	log.Debug("writing back %v", msg)
	encoder := codec.NewEncoderBytes(&buf, &mh)
	encoder.Encode(msg)
	_, err = conn.Write(buf)
	if err != nil {
		log.Error("Error writing to client %v (%v)", c.addr, err)
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
