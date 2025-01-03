package udpcloner

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/udpclone/pkg/xsync"
)

type destinations struct {
	locker          xsync.Mutex
	destinationsMap map[string]*destination
	destinations    []*destination
}

func newDestinations() *destinations {
	return &destinations{
		destinationsMap: make(map[string]*destination),
	}
}

func (dsts *destinations) AddDestination(
	ctx context.Context,
	dstString string,
	responseTimeout time.Duration,
	resolveUpdateInterval time.Duration,
) {
	logger.Debugf(ctx, "AddDestination: %s", dstString)
	dst := newDestination(dstString, responseTimeout, resolveUpdateInterval)
	dsts.locker.Do(ctx, func() {
		dsts.destinations = append(dsts.destinations, dst)
	})
}

func newDestination(
	dstString string,
	responseTimeout time.Duration,
	resolveUpdateInterval time.Duration,
) *destination {
	return &destination{
		Address:               dstString,
		ResponseTimeout:       responseTimeout,
		ResolveUpdateInterval: resolveUpdateInterval,
	}
}

func (dst *destination) ServeContext(
	ctx context.Context,
	clientsHandler clientsHandler,
) error {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			dst.locker.Do(ctx, func() {
				if dst.conn == nil {
					dst.tryConnect(ctx, clientsHandler)
				}
			})
		}
	}
}

func (dst *destination) tryConnect(
	ctx context.Context,
	clientsHandler clientsHandler,
) {
	logger.Tracef(ctx, "destination[%s].tryConnect", dst.Address)
	defer logger.Tracef(ctx, "/destination[%s].tryConnect", dst.Address)

	addr, err := net.ResolveUDPAddr("udp", dst.Address)
	if err != nil {
		logger.Errorf(ctx, "unable to resolve '%s': %v", dst, err)
		return
	}

	udpConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		logger.Tracef(ctx, "unable to connect to '%s': %v", dst.Address, err)
		return
	}

	if err := udpConn.SetWriteBuffer(bufSize); err != nil {
		logger.Warnf(ctx, "unable to set the sending UDP buffer size: %v", err)
	}

	conn := dst.setNewConn(ctx, udpConn)
	go conn.ServeConnContext(ctx, clientsHandler)
	logger.Debugf(ctx, "connected to '%s'", dst.Address)
}

func (c *destinationConn) dieIfStale(ctx context.Context) {
	dst := c.destination
	logger.Tracef(ctx, "dieIfStale: '%s'", dst.Address)
	if dst.ResponseTimeout <= 0 {
		return
	}

	lastSendTS := c.LastSendTS.Load().(time.Time)
	lastReceiveTS := c.LastReceiveTS.Load().(time.Time)
	logger.Tracef(ctx, "lastSend: %v; lastReceive: %v", lastSendTS, lastReceiveTS)
	if !lastReceiveTS.Before(lastSendTS) {
		return
	}
	if time.Since(lastReceiveTS) <= dst.ResponseTimeout {
		return
	}

	logger.Errorf(ctx, "timed out on waiting for a response from destination '%s'", dst.Address)
	go dst.Close()
}

func (c *destinationConn) reresolveAddrIfNeeded(ctx context.Context) {
	dst := c.destination
	logger.Tracef(ctx, "reresolveAddrIfNeeded: '%s'", dst.Address)

	resolveTS := dst.conn.ResolveTS.Load().(time.Time)
	if time.Since(resolveTS) <= dst.ResolveUpdateInterval {
		return
	}

	logger.Tracef(ctx, "re-resolving '%s'", dst.Address)
	udpAddr, err := net.ResolveUDPAddr("udp", dst.Address)
	if err != nil {
		logger.Errorf(ctx, "unable to resolve '%s': %v", dst.Address, err)
		return
	}
	logger.Tracef(ctx, "resolving '%s' as %s", dst.Address, udpAddr)

	if c.UDPConn.RemoteAddr().String() == udpAddr.String() {
		logger.Tracef(ctx, "the resolved address of '%s' have not changed", dst.Address)
		return
	}

	logger.Infof(ctx, "the resolved address of '%s' have changed to '%s', closing the old connection (%s)", dst.Address, udpAddr.String(), c.RemoteAddr().String())

	go dst.Close()
}

func (dsts *destinations) ForEachConnection(
	ctx context.Context,
	callback func(*destinationConn),
) {
	var destinations []*destination
	dsts.locker.Do(ctx, func() {
		destinations = copySlice(dsts.destinations)
	})

	for _, dst := range destinations {
		if dst.conn == nil {
			continue
		}
		callback(dst.conn)
	}
}

type destination struct {
	Address               string
	ResponseTimeout       time.Duration
	ResolveUpdateInterval time.Duration
	locker                xsync.Mutex
	conn                  *destinationConn
}

type destinationConn struct {
	*net.UDPConn
	sync.WaitGroup
	isClosed      atomic.Bool
	LastSendTS    atomic.Value
	LastReceiveTS atomic.Value
	ResolveTS     atomic.Value
	MessageQueue  chan []byte
	destination   *destination
}

func (dst *destination) setNewConn(
	_ context.Context,
	remote *net.UDPConn,
) *destinationConn {
	c := &destinationConn{
		UDPConn:      remote,
		MessageQueue: make(chan []byte, messageQueueSize),
		destination:  dst,
	}
	c.LastSendTS.Store(time.Now())
	c.LastReceiveTS.Store(time.Now())
	c.ResolveTS.Store(time.Now())
	dst.conn = c
	return c
}

func (c *destination) Close() (_err error) {
	ctx := context.TODO()
	logger.Debugf(ctx, "destination[%v].Close()", c.Address)
	defer func() { logger.Tracef(ctx, "/destination[%v].Close(): %v", c.Address, _err) }()
	return xsync.DoR1(ctx, &c.locker, func() error {
		if c.conn == nil {
			return ErrAlreadyClosed{}
		}
		err := c.conn.close()
		c.conn = nil
		return err
	})
}

type clientsHandler interface {
	WithClients(ctx context.Context, callback func([]*client))
	RemoveClient(context.Context, *client)
}

func (c *destinationConn) ServeConnContext(
	ctx context.Context,
	clientsHandler clientsHandler,
) (_err error) {
	var wg sync.WaitGroup
	logger.Tracef(ctx, "ServeConnContext")
	defer func() { logger.Tracef(ctx, "/ServeConnContext: %v", _err) }()

	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	errCh := make(chan error, 4)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer logger.Tracef(ctx, "the copying loop ended")
		err := c.copyTo(ctx, clientsHandler)
		if err == nil {
			errCh <- fmt.Errorf("the copying loop ended")
			return
		}
		errCh <- fmt.Errorf("unable to forward back from '%s': %w", c.destination.Address, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer logger.Tracef(ctx, "the maintenance loop ended")
		defer func() {
			errCh <- fmt.Errorf("the maintenance loop ended")
		}()

		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
			case <-t.C:
				logger.Tracef(ctx, "time to recheck if everything is OK on %v", c.destination.Address)
				c.dieIfStale(ctx)
				c.reresolveAddrIfNeeded(ctx)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer logger.Tracef(ctx, "the main loop ended")
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
			case msg := <-c.MessageQueue:
				err := c.sendMessage(ctx, msg)
				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		errCh <- nil
	}()

	err := <-errCh
	go c.destination.Close()
	cancelFn()
	wg.Wait()
	logger.Debugf(ctx, "stopped serving connection to '%s'", c.destination.Address)
	return err
}

func (c *destinationConn) sendMessage(
	_ context.Context,
	msg []byte,
) error {
	n, err := c.UDPConn.Write(msg)
	if n != len(msg) {
		return fmt.Errorf("invalid length of the sent message: %d != %d", n, len(msg))
	}
	c.LastSendTS.Store(time.Now())

	return err
}

func (c *destinationConn) copyTo(
	ctx context.Context,
	clientsHandler clientsHandler,
) (_err error) {
	logger.Debugf(ctx, "connT[%s].copyTo()", c.UDPConn.RemoteAddr().String())
	defer func() { logger.Debugf(ctx, "/connT[%s].copyTo(): %v", c.UDPConn.RemoteAddr().String(), _err) }()

	go func() {
		<-ctx.Done()
		c.UDPConn.SetReadDeadline(time.Time{})
	}()

	defer c.WaitGroup.Done()
	c.WaitGroup.Add(1)

	var buf [bufSize]byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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
				ctx := belt.WithField(ctx, "client", client.UDPAddr.String())
				client.QueueMessage(ctx, msg)
			}
		})
	}
}

func (c *destinationConn) Close() error {
	return c.destination.Close()
}

func (c *destinationConn) close() error {
	if !c.isClosed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed{}
	}
	return c.UDPConn.Close()
}

func (c *destinationConn) QueueMessage(
	ctx context.Context,
	msg []byte,
) {
	select {
	case c.MessageQueue <- msg:
	default:
		logger.Debugf(ctx, "queue for destination '%s' is full, skipping the message", c.UDPConn.RemoteAddr().String())
	}
}

type ErrAlreadyClosed struct{}

func (ErrAlreadyClosed) Error() string {
	return "already closed"
}
