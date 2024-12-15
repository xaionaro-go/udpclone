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
)

const bufSize = 1 << 20

type UDPCloner struct {
	listener *net.UDPConn

	clientAddrLocker sync.Mutex
	clientAddr       *net.UDPAddr

	destinationsLocker     sync.Mutex
	destinationConnsLocker sync.Mutex

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

func (c *UDPCloner) AddDestination(dst string) {
	c.destinationsLocker.Lock()
	defer c.destinationsLocker.Unlock()
	c.destinations = append(c.destinations, dst)
}

func (c *UDPCloner) tryCreatingMissingConns(ctx context.Context) {
	logger.Tracef(ctx, "tryCreatingMissingConns")
	defer logger.Tracef(ctx, "/tryCreatingMissingConns")

	var (
		addrs []string
		conns map[string]*connT
	)
	func() {
		c.destinationsLocker.Lock()
		defer c.destinationsLocker.Unlock()
		addrs = copySlice(c.destinations)
		conns = copyMap(c.destinationConns)
	}()

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

			conn := newConn(udpConn)
			go func(conn *connT, dst string) {
				err := conn.copyTo(ctx, c.listener, c)
				if err == nil {
					return
				}
				logger.Debugf(ctx, "unable to forward back from '%s': %v", udpConn.RemoteAddr().String(), err)
				c.destinationsLocker.Lock()
				defer c.destinationsLocker.Unlock()
				if errors.As(conn.Close(), &ErrAlreadyClosed{}) {
					return
				}
				c.delConn(dst)
				conn.Wait()
			}(conn, dst)
			logger.Debugf(ctx, "connected to '%s'", dst)
			c.setConn(dst, conn)
		}(dst)
	}

	wg.Wait()
}

func (c *UDPCloner) GetClientAddr() *net.UDPAddr {
	c.clientAddrLocker.Lock()
	defer c.clientAddrLocker.Unlock()
	return c.clientAddr
}

func (c *UDPCloner) setConn(addr string, conn *connT) {
	c.destinationConnsLocker.Lock()
	defer c.destinationConnsLocker.Unlock()
	c.destinationConns[addr] = conn
}

func (c *UDPCloner) getConn(addr string) *connT {
	c.destinationConnsLocker.Lock()
	defer c.destinationConnsLocker.Unlock()
	return c.destinationConns[addr]
}

func (c *UDPCloner) delConn(addr string) {
	c.destinationConnsLocker.Lock()
	defer c.destinationConnsLocker.Unlock()
	delete(c.destinationConns, addr)
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
		func() {
			c.clientAddrLocker.Lock()
			defer c.clientAddrLocker.Unlock()
			c.clientAddr = udpAddr
		}()

		msg := buf[:n]
		func() {
			c.destinationsLocker.Lock()
			defer c.destinationsLocker.Unlock()
			keys := make([]string, 0, len(c.destinationConns))
			for key := range c.destinationConns {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			var wg sync.WaitGroup
			for _, dst := range keys {
				wg.Add(1)
				go func(dst string) {
					defer wg.Done()
					conn := c.getConn(dst)
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
					c.delConn(dst)
					conn.Wait()
				}(dst)
			}
			wg.Wait()
		}()
	}
}
