package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/mateuszdyminski/dmap"
	"os"
	"os/signal"
	"time"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "dmapConfig", "config.toml", "Distributed map config path")
}

func main() {
	flag.Parse()

	dMap, err := dmap.NewMapFromFile(configPath)
	if err != nil {
		log.Fatalf("Can't create distributed map! Err: %v", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	for {
		select {
		case <-ticker.C:
			if err := dMap.Put("val", []byte("dupa")); err != nil {
				log.Fatalf("Can't insert value to distributed map! Err: %v", err)
			}
			log.Infof("Value %v stored!", []byte("dupa"))

			val, ok := dMap.Get("val")
			if !ok {
				log.Fatalf("Can't retrieve value from distributed map!")
			}

			log.Infof("Retrieved val: %v", val)
		case <-signals:
			ticker.Stop()
			return
		}
	}
}
