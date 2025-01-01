package udpcloner

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
)

const bufSize = 1 << 18

type UDPCloner struct {
	listener *net.UDPConn
	*clients
	*destinations
}

func New(listenAddr string) (*UDPCloner, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve '%s': %w", listenAddr, err)
	}

	listener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("unable to start listening '%s': %w", listenAddr, err)
	}
	return &UDPCloner{
		listener:     listener,
		clients:      newClients(),
		destinations: newDestinations(),
	}, nil
}

func (c *UDPCloner) ServeContext(
	ctx context.Context,
	clientResponseTimeout time.Duration,
) error {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, dst := range c.destinations.destinations {
		go dst.ServeContext(ctx, c)
	}

	var buf [bufSize]byte
	for {
		n, udpAddr, err := c.listener.ReadFromUDP(buf[:])
		if err != nil {
			return fmt.Errorf("unable to read from the listener: %w", err)
		}
		logger.Tracef(ctx, "received on the listener a message of size %d", n)
		if n >= bufSize {
			return fmt.Errorf("received too large message, not supported yet: %d >= %d", n, bufSize)
		}
		client, isNew, err := c.clients.Add(ctx, c.listener, udpAddr, clientResponseTimeout)
		if err != nil {
			logger.Errorf(ctx, "cannot add the client '%s': %v", udpAddr, err)
		}
		if isNew {
			go client.ServeContext(ctx, c)
		}
		if client != nil {
			client.LastReceiveTS.Store(time.Now())
		}

		msg := copySlice(buf[:n])
		c.destinations.ForEachConnection(ctx, func(dst *destinationConn) {
			dst.QueueMessage(ctx, msg)
		})
	}
}
