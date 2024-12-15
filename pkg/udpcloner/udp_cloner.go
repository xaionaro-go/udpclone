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

const bufSize = 1 << 20

type UDPCloner struct {
	listener *net.UDPConn

	clientAddrLocker xsync.Mutex
	clientAddr       *net.UDPAddr

	destinationsLocker     xsync.Mutex
	destinationConnsLocker xsync.Mutex

	destinations     []string
	destinationConns map[string]*connT
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
		destinationConns: make(map[string]*connT),
	}, nil
}

func (c *UDPCloner) AddDestination(
	ctx context.Context,
	dst string,
) {
	c.destinationConnsLocker.Do(ctx, func() {
		c.destinations = append(c.destinations, dst)
	})
}

func (c *UDPCloner) tryCreatingMissingConns(ctx context.Context) {
	logger.Tracef(ctx, "tryCreatingMissingConns")
	defer logger.Tracef(ctx, "/tryCreatingMissingConns")

	var (
		addrs []string
		conns map[string]*connT
	)
	c.destinationsLocker.Do(ctx, func() {
		addrs = copySlice(c.destinations)
		conns = copyMap(c.destinationConns)
	})

	var wg sync.WaitGroup
	for _, dst := range addrs {
		if _, ok := conns[dst]; ok {
			continue
		}

		wg.Add(1)
		go func(dst string) {
			defer wg.Done()

			addr, err := net.ResolveUDPAddr("udp", dst)
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

			conn := newConn(udpConn)
			go func(conn *connT, dst string) {
				err := conn.copyTo(ctx, c.listener, c)
				if err == nil {
					return
				}
				logger.Debugf(ctx, "unable to forward back from '%s': %v", udpConn.RemoteAddr().String(), err)

				c.destinationsLocker.Do(ctx, func() {
					if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
						return
					}
					c.delConn(ctx, dst)
					conn.Wait()
				})
			}(conn, dst)
			logger.Debugf(ctx, "connected to '%s'", dst)
			c.setConn(ctx, dst, conn)
		}(dst)
	}

	wg.Wait()
}

func (c *UDPCloner) GetClientAddr(ctx context.Context) *net.UDPAddr {
	return xsync.DoR1(ctx, &c.clientAddrLocker, func() *net.UDPAddr {
		return c.clientAddr
	})
}

func (c *UDPCloner) setConn(
	ctx context.Context,
	addr string,
	conn *connT,
) {
	c.destinationConnsLocker.Do(ctx, func() {
		c.destinationConns[addr] = conn
	})
}

func (c *UDPCloner) getConn(
	ctx context.Context,
	addr string,
) *connT {
	return xsync.DoR1(ctx, &c.destinationConnsLocker, func() *connT {
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

func (c *UDPCloner) ServeContext(ctx context.Context) error {
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
		c.clientAddrLocker.Do(ctx, func() {
			c.clientAddr = udpAddr
		})

		msg := buf[:n]
		c.destinationsLocker.Do(ctx, func() {
			keys := make([]string, 0, len(c.destinationConns))
			for key := range c.destinationConns {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			for _, dst := range keys {
				msg := copySlice(msg)
				go func(dst string, msg []byte) {
					conn := c.getConn(ctx, dst)
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
					conn.Wait()
				}(dst, msg)
			}
		})
	}
}
