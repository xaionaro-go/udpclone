package udpcloner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/udpclone/pkg/xsync"
)

const bufSize = 1 << 18

type UDPCloner struct {
	listener *net.UDPConn
	*clients

	destinationsLocker     xsync.Mutex
	destinationConnsLocker xsync.Mutex

	destinations     []destination
	destinationConns map[string]*destinationConn
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
		listener:         listener,
		clients:          newClients(),
		destinationConns: make(map[string]*destinationConn),
	}, nil
}

func (c *UDPCloner) AddDestination(
	ctx context.Context,
	dst string,
	responseTimeout time.Duration,
	resolveUpdateInterval time.Duration,
) {
	c.destinationConnsLocker.Do(ctx, func() {
		c.destinations = append(c.destinations, destination{
			Address:               dst,
			ResponseTimeout:       responseTimeout,
			ResolveUpdateInterval: resolveUpdateInterval,
		})
	})
}

func (c *UDPCloner) tryCreatingMissingConns(ctx context.Context) {
	logger.Tracef(ctx, "tryCreatingMissingConns")
	defer logger.Tracef(ctx, "/tryCreatingMissingConns")

	var (
		destinations []destination
		conns        map[string]*destinationConn
	)
	c.destinationsLocker.Do(ctx, func() {
		destinations = copySlice(c.destinations)
		conns = copyMap(c.destinationConns)
	})

	var wg sync.WaitGroup
	for _, dst := range destinations {
		if _, ok := conns[dst.Address]; ok {
			continue
		}

		wg.Add(1)
		go func(dst destination) {
			defer wg.Done()

			addr, err := net.ResolveUDPAddr("udp", dst.Address)
			if err != nil {
				logger.Errorf(ctx, "unable to resolve '%s': %v", dst, err)
				return
			}

			udpConn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				logger.Tracef(ctx, "unable to connect to '%s': %v", dst, err)
				return
			}

			if err := udpConn.SetWriteBuffer(bufSize); err != nil {
				logger.Warnf(ctx, "unable to set the sending UDP buffer size: %v", err)
			}
			if err := udpConn.SetWriteDeadline(time.Time{}); err != nil {
				logger.Warnf(ctx, "unable to set the sending timeout: %v", err)
			}

			conn := newDestinationConn(udpConn, dst)
			go func(conn *destinationConn, dst destination) {
				err := conn.copyTo(ctx, c.listener, c.clients)
				if err == nil {
					return
				}
				logger.Debugf(ctx, "unable to forward back from '%s': %v", udpConn.RemoteAddr().String(), err)

				c.destinationsLocker.Do(ctx, func() {
					if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
						return
					}
					c.delConn(ctx, dst.Address)
				})
				conn.Wait()
			}(conn, dst)
			logger.Debugf(ctx, "connected to '%s'", dst)
			c.setConn(ctx, dst.Address, conn)
		}(dst)
	}

	wg.Wait()
}

func (c *UDPCloner) setConn(
	ctx context.Context,
	addr string,
	conn *destinationConn,
) {
	c.destinationConnsLocker.Do(ctx, func() {
		c.destinationConns[addr] = conn
	})
}

func (c *UDPCloner) getConn(
	ctx context.Context,
	addr string,
) *destinationConn {
	return xsync.DoR1(ctx, &c.destinationConnsLocker, func() *destinationConn {
		return c.destinationConns[addr]
	})
}

func (c *UDPCloner) delConn(
	ctx context.Context,
	addr string,
) {
	c.destinationConnsLocker.Do(ctx, func() {
		delete(c.destinationConns, addr)
	})
}

func (c *UDPCloner) killStaleDestinationConns(ctx context.Context) {
	now := time.Now()

	c.destinationConnsLocker.Do(ctx, func() {
		for addr, conn := range c.destinationConns {
			if conn.ResponseTimeout <= 0 {
				continue
			}

			lastSendTS := conn.LastSendTS.Load().(time.Time)
			lastReceiveTS := conn.LastReceiveTS.Load().(time.Time)
			if !lastReceiveTS.Before(lastSendTS) {
				continue
			}
			if now.Sub(lastReceiveTS) <= conn.destination.ResponseTimeout {
				continue
			}

			logger.Errorf(ctx, "timed out on waiting for a response from destination '%s'", conn.RemoteAddr().String())
			if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
				continue
			}
			delete(c.destinationConns, addr)
		}
	})
}

func (c *UDPCloner) checkResolvedAddrs(ctx context.Context) {
	now := time.Now()
	conns := map[string]*destinationConn{}
	c.destinationConnsLocker.Do(ctx, func() {
		conns = copyMap(c.destinationConns)
	})

	for addr, conn := range conns {
		if conn.destination.ResolveUpdateInterval <= 0 {
			continue
		}

		resolveTS := conn.ResolveTS.Load().(time.Time)
		if now.Sub(resolveTS) <= conn.destination.ResolveUpdateInterval {
			continue
		}

		logger.Tracef(ctx, "re-resolving '%s'", addr)
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			logger.Errorf(ctx, "unable to resolve '%s': %v", addr, err)
			continue
		}
		logger.Tracef(ctx, "resolving '%s' as %s", addr, udpAddr)

		if conn.UDPConn.RemoteAddr().String() == udpAddr.String() {
			logger.Tracef(ctx, "the resolved address of '%s' have not changed", addr)
			continue
		}

		logger.Infof(ctx, "the resolved address of '%s' have changed to '%s', closing the old connection (%s)", addr, udpAddr.String(), conn.RemoteAddr().String())
		c.destinationConnsLocker.Do(ctx, func() {
			if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
				return
			}
			delete(c.destinationConns, addr)
		})
	}
}

func (c *UDPCloner) ServeContext(
	ctx context.Context,
	clientResponseTimeout time.Duration,
) error {
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.clients.RemoveStaleClients(ctx)
				c.killStaleDestinationConns(ctx)
				c.checkResolvedAddrs(ctx)
				c.tryCreatingMissingConns(ctx)
			}
		}
	}()

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
		err = c.clients.Add(ctx, udpAddr, clientResponseTimeout)
		if err != nil {
			logger.Errorf(ctx, "cannot add the client '%s': %v", udpAddr, err)
		}

		var addrs []string
		c.destinationConnsLocker.Do(ctx, func() {
			addrs = make([]string, 0, len(c.destinationConns))
			for key := range c.destinationConns {
				addrs = append(addrs, key)
			}
			sort.Strings(addrs)
		})

		msg := copySlice(buf[:n])
		for _, addr := range addrs {
			go func(dst string, msg []byte) {
				conn := c.getConn(ctx, dst)
				conn.LastSendTS.Store(time.Now())
				w, err := conn.Write(msg)
				if err == nil && w == n {
					logger.Tracef(ctx, "wrote a message from the listener to '%s' of size %d", dst, n)
					return
				}
				if err != nil {
					logger.Errorf(ctx, "unable to write to '%s': %v", conn.RemoteAddr().String(), err)
				} else {
					logger.Errorf(ctx, "wrote a short message to '%s': %d != %d", conn.RemoteAddr().String(), w, n)
				}
				if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
					return
				}
				c.delConn(ctx, dst)
			}(addr, msg)
		}
	}
}
