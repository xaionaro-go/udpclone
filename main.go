package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"

	"github.com/facebookincubator/go-belt"
	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/spf13/pflag"
	"github.com/xaionaro-go/udpclone/pkg/udpcloner"
	"github.com/xaionaro-go/udpclone/pkg/xsync"
)

func main() {
	loggerLevel := logger.LevelInfo
	pflag.Var(&loggerLevel, "log-level", "Log level")
	listenAddr := pflag.String("listen-addr", "0.0.0.0:9000", "")
	destinations := pflag.StringSlice("destinations", nil, "")
	netPprofAddr := pflag.String("go-net-pprof-addr", "", "address to listen to for net/pprof requests")
	pflag.Parse()

	ctx := context.Background()
	ctx = logger.CtxWithLogger(ctx, logger.Default().WithLevel(loggerLevel))
	defer belt.Flush(ctx)

	ctx = xsync.WithNoLogging(ctx, true)
	ctx = xsync.WithEnableDeadlock(ctx, false)

	if *netPprofAddr != "" {
		go func() {
			logger.Infof(ctx, "starting to listen for net/pprof requests at '%s'", *netPprofAddr)
			logger.Error(ctx, http.ListenAndServe(*netPprofAddr, nil))
		}()
	}

	cloner, err := udpcloner.New(*listenAddr)
	if err != nil {
		logger.Panicf(ctx, "unable to initialize the cloner: %v", err)
	}

	for _, dst := range *destinations {
		cloner.AddDestination(ctx, dst)
	}

	logger.Infof(ctx, "started")
	err = cloner.ServeContext(ctx)
	if err != nil {
		logger.Panicf(ctx, "unable to serve: %v", err)
	}
}
