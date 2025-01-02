package udpcloner

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/udpclone/pkg/xsync"
)

const (
	maxClients       = 30 // we currently limit the amount of clients because of the ugly algo how we remove a client.
	messageQueueSize = 1024
)

type clients struct {
	locker     xsync.Mutex
	clientsMap map[string]*client
	clients    []*client
}

func newClients() *clients {
	return &clients{
		clientsMap: make(map[string]*client),
	}
}

func (c *clients) CreateOrGet(
	ctx context.Context,
	udpListener *net.UDPConn,
	udpAddr *net.UDPAddr,
	responseTimeout time.Duration,
) (*client, bool, error) {
	return xsync.DoR3(ctx, &c.locker, func() (*client, bool, error) {
		key := udpAddr.String()
		if client, ok := c.clientsMap[key]; ok {
			return client, false, nil
		}
		if len(c.clients) >= maxClients {
			return nil, false, fmt.Errorf("maximum clients (%d) reached, cannot add more", maxClients)
		}
		client := newClient(udpListener, udpAddr, responseTimeout)

		c.clientsMap[key] = client
		c.clients = append(c.clients, client)
		return client, true, nil
	})
}

func (c *clients) WithClients(ctx context.Context, callback func([]*client)) {
	c.locker.RDo(ctx, func() {
		callback(c.clients)
	})
}

func (c *clients) RemoveClient(
	ctx context.Context,
	client *client,
) {
	c.locker.Do(ctx, func() {
		c.removeClient(ctx, client)
	})
}

func (c *clients) removeClient(
	ctx context.Context,
	client *client,
) {
	logger.Debugf(ctx, "removing client '%s'", client.UDPAddr.String())
	key := client.UDPAddr.String()
	delete(c.clientsMap, key)
	c.clients = c.clients[:0]
	for _, client := range c.clientsMap {
		c.clients = append(c.clients, client)
	}
}

func (c *client) killIfStale(
	ctx context.Context,
	clientsHandler clientsHandler,
	cancelFn context.CancelFunc,
) {
	if c.ResponseTimeout <= 0 {
		return
	}

	lastSendTS := c.LastSendTS.Load().(time.Time)
	lastReceiveTS := c.LastReceiveTS.Load().(time.Time)
	logger.Tracef(ctx, "lastSend: %v; lastReceive: %v", lastSendTS, lastReceiveTS)
	if !lastReceiveTS.Before(lastSendTS) {
		return
	}
	if time.Since(lastReceiveTS) <= c.ResponseTimeout {
		return
	}

	logger.Errorf(ctx, "timed out on waiting for a response from client '%s'", c.UDPAddr.String())
	go clientsHandler.RemoveClient(ctx, c)
	cancelFn()
}

type client struct {
	*net.UDPAddr
	ListenerConn    *net.UDPConn
	ResponseTimeout time.Duration
	LastSendTS      atomic.Value
	LastReceiveTS   atomic.Value
	MessageQueue    chan []byte
}

func newClient(
	udpListener *net.UDPConn,
	udpAddr *net.UDPAddr,
	responseTimeout time.Duration,
) *client {
	c := &client{
		ListenerConn:    udpListener,
		UDPAddr:         udpAddr,
		ResponseTimeout: responseTimeout,
		MessageQueue:    make(chan []byte, messageQueueSize),
	}
	c.LastSendTS.Store(time.Now())
	c.LastReceiveTS.Store(time.Now())
	return c
}

func (c *client) QueueMessage(
	ctx context.Context,
	msg []byte,
) {
	select {
	case c.MessageQueue <- msg:
	default:
		logger.Debugf(ctx, "queue for client '%s' is full, skipping the message", c.UDPAddr)
	}
}

func (c *client) ServeContext(
	ctx context.Context,
	clientsHandler clientsHandler,
) {
	logger.Debugf(ctx, "ServeContext: '%s'", c.UDPAddr)
	defer logger.Tracef(ctx, "/ServeContext: '%s'", c.UDPAddr)
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				logger.Debugf(ctx, "the maintenance loop ended")
				return
			case <-t.C:
				c.killIfStale(ctx, clientsHandler, cancelFn)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Debugf(ctx, "the sender loop ended")
				return
			case msg := <-c.MessageQueue:
				err := c.sendMessage(ctx, msg)
				if err != nil {
					logger.Infof(ctx, "unable to send to '%s': %v", c.UDPAddr, err)
					go clientsHandler.RemoveClient(ctx, c)
					cancelFn()
				}
			}
		}
	}()

	wg.Wait()
}

func (c *client) sendMessage(
	ctx context.Context,
	msg []byte,
) error {
	addr := c.UDPAddr
	w, err := c.ListenerConn.WriteToUDP(msg, addr)
	if err != nil {
		return fmt.Errorf("unable to write to '%s': %w", addr.String(), err)
	}
	logger.Tracef(ctx, "wrote a message to client '%s' of size %d", addr, len(msg))
	if w != len(msg) {
		return fmt.Errorf("wrote a short message to '%s': %d != %d", addr.String(), w, len(msg))
	}
	c.LastSendTS.Store(time.Now())
	return nil
}
