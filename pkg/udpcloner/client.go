package udpcloner

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/udpclone/pkg/xsync"
)

const (
	maxClients = 30 // we currently limit the amount of clients because of the ugly algo how we remove a client.
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

func (c *clients) Add(
	ctx context.Context,
	udpAddr *net.UDPAddr,
	responseTimeout time.Duration,
) error {
	return xsync.DoR1(ctx, &c.locker, func() error {
		key := udpAddr.String()
		if _, ok := c.clientsMap[key]; ok {
			return nil
		}
		if len(c.clients) >= maxClients {
			return fmt.Errorf("maximum clients (%d) reached, cannot add more", maxClients)
		}
		client := newClient(udpAddr, responseTimeout)
		c.clientsMap[key] = client
		c.clients = append(c.clients, client)
		return nil
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
	for _, addr := range c.clientsMap {
		c.clients = append(c.clients, addr)
	}
}

func (c *clients) RemoveStaleClients(
	ctx context.Context,
) {
	now := time.Now()

	c.locker.Do(ctx, func() {
		for _, client := range c.clients {
			if client.ResponseTimeout <= 0 {
				continue
			}

			lastSendTS := client.LastSendTS.Load().(time.Time)
			lastReceiveTS := client.LastReceiveTS.Load().(time.Time)
			if !lastReceiveTS.Before(lastSendTS) {
				continue
			}
			if now.Sub(lastReceiveTS) <= client.ResponseTimeout {
				continue
			}

			logger.Errorf(ctx, "timed out on waiting for a response from client '%s'", client.UDPAddr.String())
			c.removeClient(ctx, client)
		}
	})
}

type client struct {
	*net.UDPAddr
	ResponseTimeout time.Duration
	LastSendTS      atomic.Value
	LastReceiveTS   atomic.Value
}

func newClient(
	udpAddr *net.UDPAddr,
	responseTimeout time.Duration,
) *client {
	c := &client{
		UDPAddr:         udpAddr,
		ResponseTimeout: responseTimeout,
	}
	c.LastSendTS.Store(time.Now())
	c.LastReceiveTS.Store(time.Now())
	return c
}
