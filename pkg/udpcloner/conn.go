package udpcloner

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/facebookincubator/go-belt/tool/logger"
)

type connT struct {
	*net.UDPConn
	sync.WaitGroup
	isClosed atomic.Bool
}

func newConn(
	remote *net.UDPConn,
) *connT {
	return &connT{
		UDPConn: remote,
	}
}

type clientAddrGetter interface {
	GetClientAddr(context.Context) *net.UDPAddr
}

func (c *connT) copyTo(
	ctx context.Context,
	target *net.UDPConn,
	clientAddrGetter clientAddrGetter,
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

		msg := buf[:n]
		addr := clientAddrGetter.GetClientAddr(ctx)
		w, err := target.WriteToUDP(msg, addr)
		if err != nil {
			return fmt.Errorf("unable to write to '%s': %w", addr.String(), err)
		}
		logger.Tracef(ctx, "wrote a message from the connection '%s' to '%s' of size %d", c.RemoteAddr(), addr, n)
		if w != n {
			return fmt.Errorf("wrote a short message to '%s': %d != %d", addr.String(), w, n)
		}
	}
}

func (c *connT) Close() error {
	if !c.isClosed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed{}
	}
	return c.UDPConn.Close()
}

type ErrAlreadyClosed struct{}

func (ErrAlreadyClosed) Error() string {
	return "already closed"
}
