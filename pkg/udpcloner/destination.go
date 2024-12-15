package udpcloner

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
)

type destination struct {
	Address               string
	ResponseTimeout       time.Duration
	ResolveUpdateInterval time.Duration
}

type destinationConn struct {
	*net.UDPConn
	destination
	sync.WaitGroup
	isClosed      atomic.Bool
	LastSendTS    atomic.Value
	LastReceiveTS atomic.Value
	ResolveTS     atomic.Value
}

func newDestinationConn(
	remote *net.UDPConn,
	destination destination,
) *destinationConn {
	c := &destinationConn{
		destination: destination,
		UDPConn:     remote,
	}
	c.LastSendTS.Store(time.Now())
	c.LastReceiveTS.Store(time.Now())
	c.ResolveTS.Store(time.Now())
	return c
}

type clientsHandler interface {
	WithClients(ctx context.Context, callback func([]*client))
	RemoveClient(context.Context, *client)
}

func (c *destinationConn) copyTo(
	ctx context.Context,
	target *net.UDPConn,
	clientsHandler clientsHandler,
) (_err error) {
	logger.Debugf(ctx, "connT[%s].copyTo()", c.UDPConn.RemoteAddr().String())
	defer logger.Debugf(ctx, "/connT[%s].copyTo(): %v", c.UDPConn.RemoteAddr().String(), _err)

	defer c.WaitGroup.Done()
	c.WaitGroup.Add(1)

	var buf [bufSize]byte
	for {
		n, _, err := c.UDPConn.ReadFromUDP(buf[:])
		if err != nil {
			return fmt.Errorf("unable to read from the listener: %w", err)
		}
		logger.Tracef(ctx, "received a message from the connection '%s' of size %d", c.RemoteAddr(), n)
		if n >= bufSize {
			return fmt.Errorf("received too large message, not supported yet: %d >= %d", n, bufSize)
		}
		c.LastReceiveTS.Store(time.Now())

		msg := copySlice(buf[:n])
		clientsHandler.WithClients(ctx, func(clients []*client) {
			for _, client := range clients {
				addr := client.UDPAddr
				w, err := target.WriteToUDP(msg, addr)
				if err != nil {
					logger.Errorf(ctx, "unable to write to '%s': %w", addr.String(), err)
					go clientsHandler.RemoveClient(ctx, client)
					return
				}
				logger.Tracef(ctx, "wrote a message from the connection '%s' to '%s' of size %d", c.RemoteAddr(), addr, n)
				if w != n {
					logger.Errorf(ctx, "wrote a short message to '%s': %d != %d", addr.String(), w, n)
					go clientsHandler.RemoveClient(ctx, client)
					return
				}
			}
		})
	}
}

func (c *destinationConn) Close() error {
	if !c.isClosed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed{}
	}
	return c.UDPConn.Close()
}

type ErrAlreadyClosed struct{}

func (ErrAlreadyClosed) Error() string {
	return "already closed"
}
