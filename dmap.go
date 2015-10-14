package dmap

import (
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"github.com/mateuszdyminski/dmap/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"sync"
	"time"
	"sync/atomic"
)

const (
	OK    = 200
	ERROR = 400

	PUT = 1
	DEL = 2

	CONSISTENCY_ALL = "all"
	CONSISTENCY_QUORUM = "quorum"
)

func NewMap(config DMapConfig) (DMap, error) {
	if config.HostAddress == "" {
		return nil, fmt.Errorf("Please provide HOST entry in config!")
	}

	if config.Members == nil || len(config.Members) == 0 {
		return nil, fmt.Errorf("Please provide at least one MEMBER entry in config!")
	}

	var members []Member
	for _, m := range config.Members {
		members = append(members, Member{address: m, active: false})
	}

	if config.Consistency == "" {
		log.Infof("Consistency is not set. Setting to default value: ALL")
		config.Consistency = CONSISTENCY_ALL
	} else if config.Consistency != CONSISTENCY_ALL && config.Consistency != CONSISTENCY_QUORUM {
		log.Infof("Wrong value of consistency. Setting to default value: ALL")
		config.Consistency = CONSISTENCY_ALL
	}

	service := dMapService{data: make(map[string][]byte, 0), members: members, config: config}
	service.startHealthChecker()

	dm := DMapImpl{config: config, service: &service}
	dm.startServer()

	return &dm, nil
}

func NewMapFromFile(configPath string) (DMap, error) {
	bytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("Can't open config file! Err: %v", err)
	}

	var conf DMapConfig
	if err := toml.Unmarshal(bytes, &conf); err != nil {
		return nil, fmt.Errorf("Can't unmarshal config file! Err: %v", err)
	}

	return NewMap(conf)
}

type DMapConfig struct {
	HostAddress string
	Consistency string
	Members     []string
}

type dMapService struct {
	m        sync.Mutex
	config   DMapConfig
	data     map[string][]byte
	members  []Member
	quitChan chan struct{}
}

func (s *dMapService) Send(ctx context.Context, in *protos.Message) (*protos.Response, error) {
	log.Infof("Received msg: %+v", in)

	s.m.Lock()

	switch in.OpType {
	case (PUT):
		s.data[in.Key] = in.Value
	case (DEL):
		delete(s.data, in.Key)
	}

	defer s.m.Unlock()

	return &protos.Response{Code: OK}, nil
}

func (s *dMapService) Ping(ctx context.Context, in *protos.HealthCheck) (*protos.Response, error) {
	log.Infof("Received health check request: %+v", in.Origin)
	return &protos.Response{Code: OK}, nil
}

type DMapImpl struct {
	service *dMapService
	config  DMapConfig
	server *grpc.Server
}

type Member struct {
	address string
	active  bool
}

type DMap interface {
	Get(string) ([]byte, bool)
	Put(string, []byte) error
	Exist(string) bool
	Del(string) error
	Close() error
}

func (d *DMapImpl) Get(key string) ([]byte, bool) {
	val, ok := d.service.data[key]
	return val, ok
}

func (d *DMapImpl) Put(key string, value []byte) error {
	d.service.m.Lock()
	defer d.service.m.Unlock()

	// put on remote members
	summary := d.service.sendToAll(key, value)

	// put locally
	d.service.data[key] = value
	summary.broadcastCount++

	ok, err := summary.isSuccess()
	if !ok {
		return err
	}

	return nil
}
func (d *DMapImpl) Exist(key string) bool {
	_, ok := d.service.data[key]

	// TODO: if not found locally - try to fetch from remote members

	return ok
}

func (d *DMapImpl) Del(key string) error {
	d.service.m.Lock()
	defer d.service.m.Unlock()

	// delete on remote members
	summary := d.service.deleteInAll(key)

	// delete locally
	delete(d.service.data, key)
	summary.broadcastCount++
	ok, err := summary.isSuccess()
	if !ok {
		return err
	}

	return nil
}

func (d *DMapImpl) Close() error {
	d.service.quitChan <- struct{}{}
	d.server.Stop()

	return nil
}

func (d *DMapImpl) startServer() error {
	lis, err := net.Listen("tcp", d.config.HostAddress)
	if err != nil {
		return fmt.Errorf("Failed to start grpc server: %v", err)
	}

	d.server = grpc.NewServer()
	protos.RegisterDMapServer(d.server, d.service)
	log.Infof("Starting grpc server... \n")
	go d.server.Serve(lis)

	return nil
}

type broadcastSummary struct {
	consistency string
	broadcastCount int32
	broadcastErrorCount int32
}

func (b *broadcastSummary) isSuccess() (bool, error) {
	switch(b.consistency) {
	case(CONSISTENCY_ALL):
		if b.broadcastErrorCount == 0 {
			return true, nil
		}
		return false, fmt.Errorf("Broadcast failed! Lack of consistency. Success: %d, Errors: %d", b.broadcastCount, b.broadcastErrorCount)
	case(CONSISTENCY_QUORUM):
		if float64(b.broadcastErrorCount) < float64(b.broadcastCount) / float64(2) {
			return true, nil
		}
		return false, fmt.Errorf("Broadcast failed! Lack of consistency. Success: %d, Errors: %d", b.broadcastCount, b.broadcastErrorCount)
	}

	return false, fmt.Errorf("Wrong counsistency type: %s", b.consistency)
}

func (d *dMapService) broadcast(msg *protos.Message) broadcastSummary {
	summary := broadcastSummary{broadcastCount: int32(len(d.members)), consistency: d.config.Consistency}
	wg := sync.WaitGroup{}
	wg.Add(len(d.members))
	for i := range d.members {
		go func(m *Member, msg *protos.Message) {
			err := send(m, msg)
			if err != nil {
				atomic.AddInt32(&summary.broadcastErrorCount, 1)
				log.Error(err)
			}
			wg.Done()
		}(&d.members[i], msg)
	}

	wg.Wait()
	log.Infof("Broadcast to all members finished!")

	return summary
}

func send(m *Member, msg *protos.Message) error {
	if !m.active {
		return fmt.Errorf("Ommitting member %s due to inactive status.", m.address)
	}

	conn, err := grpc.Dial(m.address, grpc.WithTimeout(1000 * time.Millisecond))
	if err != nil {
		m.active = false
		return err
	}
	defer conn.Close()

	client := protos.NewDMapClient(conn)
	resp, err := client.Send(context.Background(), msg)
	if resp.Code != OK {
		return fmt.Errorf("Can't update all peers! Status code: %d", resp.Code)
	}

	return nil
}

func (d *dMapService) sendToAll(key string, value []byte) broadcastSummary {
	message := &protos.Message{
		Key:    key,
		Value:  value,
		OpType: DEL,
	}

	return d.broadcast(message)
}

func (d *dMapService) deleteInAll(key string) broadcastSummary {
	message := &protos.Message{
		Key:    key,
		OpType: DEL,
	}

	return d.broadcast(message)
}

func (d *dMapService) startHealthChecker() {
	ticker := time.NewTicker(5 * time.Second)
	d.quitChan = make(chan struct{})

	go func() {

		for {
			select {
			case <-ticker.C:
				log.Infof("Start checking members status")

				wg := sync.WaitGroup{}
				d.m.Lock()
				wg.Add(len(d.members))
				for i := range d.members {
					go func(m *Member) {
						err := checkHealth(d.config.HostAddress, m)
						if err != nil {
							log.Error(err)
						}
						wg.Done()
					}(&d.members[i])
				}

				wg.Wait()
				log.Infof("Checking members status finished!")
				d.m.Unlock()
			case <-d.quitChan:
				ticker.Stop()
				return
			}
		}
	}()
}

func checkHealth(origin string, m *Member) error {
	conn, err := grpc.Dial(m.address, grpc.WithTimeout(100 * time.Millisecond))
	if err != nil {
		m.active = false
		return fmt.Errorf("Member %s is unavailable! Err: %v", m.address, err)
	}
	defer conn.Close()

	client := protos.NewDMapClient(conn)
	message := &protos.HealthCheck{
		Origin: origin,
	}

	if resp, err := client.Ping(context.Background(), message); resp.Code != OK || err != nil {
		m.active = false
		return fmt.Errorf("Member %s is unavailable! Code: %d. Err: %v", m.address, resp.Code, err)
	} else {
		m.active = true
		log.Infof("Member %s active", m.address)
	}

	return nil
}