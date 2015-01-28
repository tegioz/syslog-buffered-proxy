package main

import (
	"flag"
	"fmt"
	"github.com/jeromer/syslogparser"
	syslogd "github.com/mcuadros/go-syslog"
	"log"
	"net"
	"time"
)

var config struct {
	listenOn  string
	forwardTo string
}

func init() {
	flag.StringVar(&config.listenOn, "listen-on", "localhost:2514", "host:port to listen for incoming logs")
	flag.StringVar(&config.forwardTo, "forward-to", "localhost:1514", "host:port to forward the logs to (must be a syslog tcp destination)")
}

type listener struct {
	listenOn string
	channel  syslogd.LogPartsChannel
	server   *syslogd.Server
}

func (l *listener) init() {
	l.channel = make(syslogd.LogPartsChannel)
	handler := syslogd.NewChannelHandler(l.channel)
	l.server = syslogd.NewServer()
	l.server.SetFormat(syslogd.RFC5424)
	l.server.SetHandler(handler)
	l.server.ListenUDP(l.listenOn)
	l.server.ListenTCP(l.listenOn)
	l.server.Boot()
}

type forwarder struct {
	forwardTo              string
	store                  []syslogparser.LogParts
	listenerCh             syslogd.LogPartsChannel
	forwardCh              syslogd.LogPartsChannel
	reinjectCh             syslogd.LogPartsChannel
	destination            net.Conn
	connectedToDestination bool
}

func (f *forwarder) init() {
	f.store = make([]syslogparser.LogParts, 0)
	f.forwardCh = make(chan syslogparser.LogParts)
	f.reinjectCh = make(chan syslogparser.LogParts)
	f.start()
}

func (f *forwarder) connectToDestination() {
	var err error
	f.connectedToDestination = false
	go func() {
		for !f.connectedToDestination {
			f.destination, err = net.Dial("tcp", f.forwardTo)
			if err != nil {
				log.Println("Error connecting to destination host:", err)
				time.Sleep(10 * time.Second)
			} else {
				log.Println("Forwarder connected to destination host")
				f.connectedToDestination = true
			}
		}
	}()
}

func (f *forwarder) start() {
	f.connectToDestination()

	go func() {
		// Forward log entries to the destination host as they arrive to the forward channel
		for logEntry := range f.forwardCh {
			f.forwardLogEntry(logEntry)
		}
	}()

	go func() {
		for {
			select {
			// Store log entries as they arrive from the listener or are reinjected
			case logEntry := <-f.listenerCh:
				log.Println("Log entry received:", logEntry)
				f.store = append(f.store, logEntry)
			case logEntry := <-f.reinjectCh:
				log.Println("Log entry reinjected:", logEntry)
				f.store = append(f.store, logEntry)
				copy(f.store[1:], f.store[0:])
				f.store[0] = logEntry
			default:
				// Forward them if we are connected to the destination server
				if f.connectedToDestination && len(f.store) > 0 {
					logEntry := f.store[0]
					f.store = f.store[1:len(f.store)]
					log.Println("Getting log entry from store:", logEntry)
					f.forwardCh <- logEntry
				}
			}
		}
	}()
}

func (f *forwarder) forwardLogEntry(le syslogparser.LogParts) {
	// Forward log entry using RFC5424 format
	// Must match syslog's expected datetime format. Need to transform to RFC3339
	_, err := fmt.Fprintf(
		f.destination,
		"<%d>%d %s %s %s %s %s\n",
		le["priority"], 1, le["timestamp"].(time.Time).Format(time.RFC3339), le["hostname"], le["app_name"], le["proc_id"], le["message"],
	)
	if err != nil {
		// LogEntry wasn't forwarded successfully, reinject it in the store and try to reconnect
		log.Println("Error forwarding log entry:", le, err)
		f.reinjectCh <- le
		f.connectToDestination()
	} else {
		log.Println("Log entry forwarded:", le)
	}
}

func main() {
	flag.Parse()

	// Setup syslog server listening on the port provided (tcp and udp)
	listener := &listener{
		listenOn: config.listenOn,
	}
	listener.init()

	// Setup forwarder component. It will accumulate log entries received from the
	// listener, forwarding them to the destination server when it's available
	forwarder := &forwarder{
		listenerCh: listener.channel,
		forwardTo:  config.forwardTo,
	}
	forwarder.init()

	log.Println("Syslog buffer collecting logs..")
	listener.server.Wait()
}
