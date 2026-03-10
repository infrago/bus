package bus

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/infrago/infra"
)

func init() {
	infra.Register(infra.DEFAULT, &defaultBusDriver{})
}

var (
	errBusRunning       = errors.New("bus is running")
	errBusNotRunning    = errors.New("bus is not running")
	errBusInvalidTarget = errors.New("invalid bus target")
)

type (
	defaultBusDriver struct{}

	defaultBusConnection struct {
		mutex        sync.RWMutex
		running      bool
		services     map[string]struct{}
		messages     map[string]struct{}
		instance     *Instance
		prefix       string
		serviceRetry map[string][]time.Duration
	}
)

// Connect establishes an in-memory
func (driver *defaultBusDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultBusConnection{
		services:     make(map[string]struct{}, 0),
		messages:     make(map[string]struct{}, 0),
		instance:     inst,
		prefix:       inst.Config.Prefix,
		serviceRetry: make(map[string][]time.Duration, 0),
	}, nil
}

func (c *defaultBusConnection) Open() error  { return nil }
func (c *defaultBusConnection) Close() error { return nil }

func (c *defaultBusConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return errBusRunning
	}

	c.running = true
	return nil
}

func (c *defaultBusConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return errBusNotRunning
	}

	c.running = false
	return nil
}

func (c *defaultBusConnection) RegisterService(subject string, retries []time.Duration) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if subject == "" {
		return errBusInvalidTarget
	}

	c.services[subject] = struct{}{}
	c.serviceRetry[subject] = append([]time.Duration{}, retries...)

	return nil
}

func (c *defaultBusConnection) RegisterMessage(subject string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if subject == "" {
		return errBusInvalidTarget
	}

	c.messages[subject] = struct{}{}

	return nil
}

// Request handles synchronous call - for in-memory bus, directly invoke local.
func (c *defaultBusConnection) Request(_ string, data []byte, _ time.Duration) ([]byte, error) {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	return c.instance.HandleCall(data)
}

// Publish broadcasts message to all local handlers - for in-memory, invoke local.
func (c *defaultBusConnection) Publish(_ string, data []byte) error {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	return c.instance.HandleMessage(data)
}

// Enqueue handles queued call - for in-memory bus, directly invoke local.
func (c *defaultBusConnection) Enqueue(subject string, data []byte) error {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	if strings.HasPrefix(subject, "publish.") {
		return c.instance.HandleMessage(data)
	}
	service := strings.TrimPrefix(subject, "queue.")
	c.dispatchService(data, 1, append([]time.Duration{}, c.serviceRetry[service]...))
	return nil
}

// Stats returns empty stats for in-memory
func (c *defaultBusConnection) Stats() []infra.ServiceStats {
	return nil
}

func (c *defaultBusConnection) ListNodes() []infra.NodeInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	identity := infra.Identity()
	project := identity.Project
	if project == "" {
		project = infra.INFRAGO
	}
	names := make([]string, 0, len(c.services))
	for name := range c.services {
		names = append(names, c.serviceName(name))
	}
	sort.Strings(names)

	return []infra.NodeInfo{
		{
			Project:  project,
			Node:     identity.Node,
			Role:     identity.Role,
			Profile:  identity.Profile,
			Services: names,
			Updated:  time.Now().UnixMilli(),
		},
	}
}

func (c *defaultBusConnection) ListServices() []infra.ServiceInfo {
	nodes := c.ListNodes()
	if len(nodes) == 0 {
		return nil
	}
	merged := make(map[string]*infra.ServiceInfo)
	for _, node := range nodes {
		for _, svc := range node.Services {
			svcKey := svc
			info, ok := merged[svcKey]
			if !ok {
				info = &infra.ServiceInfo{Service: svc, Name: svc}
				merged[svcKey] = info
			}
			info.Nodes = append(info.Nodes, infra.ServiceNode{
				Node:    node.Node,
				Role:    node.Role,
				Profile: node.Profile,
			})
			if node.Updated > info.Updated {
				info.Updated = node.Updated
			}
		}
	}

	out := make([]infra.ServiceInfo, 0, len(merged))
	for _, info := range merged {
		info.Instances = len(info.Nodes)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

func (c *defaultBusConnection) serviceName(subject string) string {
	if c.prefix == "" {
		return subject
	}
	return strings.TrimPrefix(subject, c.prefix)
}

func (c *defaultBusConnection) dispatchService(data []byte, attempt int, retries []time.Duration) {
	if c.instance == nil {
		c.instance = &Instance{}
	}

	err := c.instance.HandleServiceAsync(data, attempt, DispatchFinal(retries, attempt))
	if err == nil || !IsRetryableDispatchError(err) {
		return
	}

	delay, ok := DispatchRetryDelay(retries, attempt)
	if !ok {
		return
	}
	time.AfterFunc(delay, func() {
		c.mutex.RLock()
		running := c.running
		c.mutex.RUnlock()
		if !running {
			return
		}
		c.dispatchService(data, attempt+1, retries)
	})
}
