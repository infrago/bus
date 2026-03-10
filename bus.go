package bus

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	base "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	errBusNotReady = errors.New("bus is not ready")
	errBusReserved = errors.New("bus name uses reserved prefix")
)

var (
	module = &busModule{
		drivers:     make(map[string]Driver, 0),
		configs:     make(map[string]Config, 0),
		connections: make(map[string]Connection, 0),
		weights:     make(map[string]int, 0),
		services:    make(map[string]serviceMeta, 0),
		messages:    make(map[string]serviceMeta, 0),
	}
	host = infra.Mount(module)
)

type (
	// Handler processes incoming payload and returns reply bytes for call.
	Handler func([]byte) ([]byte, error)

	// Driver connections a bus transport.
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	// Connection defines a bus transport connection.
	Connection interface {
		Open() error
		Close() error
		Start() error
		Stop() error

		RegisterService(subject string, retries []time.Duration) error
		RegisterMessage(subject string) error

		Request(subject string, data []byte, timeout time.Duration) ([]byte, error)
		Publish(subject string, data []byte) error
		Enqueue(subject string, data []byte) error

		Stats() []infra.ServiceStats
		ListNodes() []infra.NodeInfo
		ListServices() []infra.ServiceInfo
	}

	busModule struct {
		mutex sync.RWMutex

		drivers     map[string]Driver
		configs     map[string]Config
		connections map[string]Connection
		weights     map[string]int
		wrr         *util.WRR
		services    map[string]serviceMeta
		messages    map[string]serviceMeta

		opened  bool
		started bool
	}

	Instance struct {
		Name   string
		Config Config
	}

	Config struct {
		Driver  string
		Weight  int
		Prefix  string
		Group   string
		Setting base.Map
	}

	Configs map[string]Config

	serviceMeta struct {
		name          string
		desc          string
		dispatchRetry []time.Duration
	}

	DispatchError struct {
		res base.Res
	}
)

const (
	subjectCall  = "call"
	subjectQueue = "queue"
	subjectEvent = "event"
	subjectGroup = "publish"

	// names with this prefix are reserved for bus internal control channels.
	reservedNamePrefix = "_"
)

type (
	// busRequest combines metadata and payload for transmission.
	busRequest struct {
		infra.Metadata
		Name    string   `json:"name"`
		Payload base.Map `json:"payload,omitempty"`
	}

	// busResponse contains result with full Res info.
	busResponse struct {
		Code   int    `json:"code"`
		Status string `json:"status"`
		Result string `json:"result,omitempty"`
		// Legacy compatibility with older nodes.
		State string   `json:"state,omitempty"`
		Desc  string   `json:"desc,omitempty"`
		Time  int64    `json:"time"`
		Data  base.Map `json:"data,omitempty"`
	}
)

// Register dispatches registrations.
func (m *busModule) Register(name string, value base.Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	case infra.Service:
		m.RegisterService(name, v)
	case infra.Services:
		for key, svc := range v {
			target := key
			if name != "" {
				target = name + "." + key
			}
			m.RegisterService(target, svc)
		}
	case infra.Message:
		m.RegisterMessage(name, v)
	case infra.Messages:
		for key, msg := range v {
			target := key
			if name != "" {
				target = name + "." + key
			}
			m.RegisterMessage(target, msg)
		}
	}
}

// RegisterDriver registers a bus driver.
func (m *busModule) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}
	if driver == nil {
		panic("Invalid bus driver: " + name)
	}
	if _, ok := m.drivers[name]; ok {
		panic("Bus driver already registered: " + name)
	}
	m.drivers[name] = driver
}

// RegisterConfig registers a named bus config.
// If name is empty, it uses DEFAULT.
func (m *busModule) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	if name == "" {
		name = infra.DEFAULT
	}
	if _, ok := m.configs[name]; ok {
		panic("Bus config already registered: " + name)
	}
	m.configs[name] = cfg
}

// RegisterConfigs registers multiple named bus configs.
func (m *busModule) RegisterConfigs(configs Configs) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	for name, cfg := range configs {
		if name == "" {
			name = infra.DEFAULT
		}
		if _, ok := m.configs[name]; ok {
			panic("Bus config already registered: " + name)
		}
		m.configs[name] = cfg
	}
}

// RegisterService binds service name into bus subjects.
func (m *busModule) RegisterService(name string, svc infra.Service) {
	if name == "" {
		return
	}
	if err := validateBusName(name); err != nil {
		panic(err.Error() + ": " + name)
	}
	m.mutex.Lock()
	m.services[name] = serviceMeta{
		name:          svc.Name,
		desc:          svc.Desc,
		dispatchRetry: normalizeDurations(svc.Retry),
	}
	m.mutex.Unlock()
}

// RegisterMessage binds message name into bus subjects.
func (m *busModule) RegisterMessage(name string, msg infra.Message) {
	if name == "" {
		return
	}
	if err := validateBusName(name); err != nil {
		panic(err.Error() + ": " + name)
	}
	m.mutex.Lock()
	m.messages[name] = serviceMeta{name: msg.Name, desc: msg.Desc}
	m.mutex.Unlock()
}

func (m *busModule) Config(global base.Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	cfgAny, ok := global["bus"]
	if !ok {
		return
	}
	cfgMap, ok := castToMap(cfgAny)
	if !ok || cfgMap == nil {
		return
	}

	rootConfig := base.Map{}
	for key, val := range cfgMap {
		if conf, ok := castToMap(val); ok && key != "setting" {
			m.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}
	if len(rootConfig) > 0 {
		m.configure(infra.DEFAULT, rootConfig)
	}
}

func (m *busModule) configure(name string, conf base.Map) {
	cfg := Config{}
	if existing, ok := m.configs[name]; ok {
		cfg = existing
	}

	if v, ok := conf["driver"].(string); ok && v != "" {
		cfg.Driver = v
	}
	if v, ok := conf["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := conf["group"].(string); ok {
		cfg.Group = v
	}
	if v, ok := parseWeight(conf["weight"]); ok {
		cfg.Weight = v
	}
	if v, ok := castToMap(conf["setting"]); ok {
		cfg.Setting = v
	}

	m.configs[name] = cfg
}

// Setup initializes defaults.
func (m *busModule) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	if len(m.configs) == 0 {
		m.configs[infra.DEFAULT] = Config{Driver: infra.DEFAULT, Weight: 1}
	}

	// normalize configs
	for name, cfg := range m.configs {
		if name == "" {
			name = infra.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = infra.DEFAULT
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		if strings.TrimSpace(cfg.Group) == "" {
			cfg.Group = strings.TrimSpace(infra.Identity().Role)
		}
		cfg.Prefix = normalizePrefix(cfg.Prefix)
		m.configs[name] = cfg
	}
}

// Open connections bus and registers services.
func (m *busModule) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if len(m.configs) == 0 {
		panic("Missing bus config")
	}

	for name, cfg := range m.configs {
		driver, ok := m.drivers[cfg.Driver]
		if !ok || driver == nil {
			panic("Missing bus driver: " + cfg.Driver)
		}

		if cfg.Weight == 0 {
			cfg.Weight = 1
		}

		inst := &Instance{Name: name, Config: cfg}
		conn, err := driver.Connect(inst)
		if err != nil {
			panic("Failed to connect to bus: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("Failed to open bus: " + err.Error())
		}

		for svc, meta := range m.services {
			base := m.subjectBase(cfg.Prefix, svc)
			if err := conn.RegisterService(base, meta.dispatchRetry); err != nil {
				panic("Failed to register bus: " + err.Error())
			}
		}
		for msg := range m.messages {
			base := m.subjectBase(cfg.Prefix, msg)
			if err := conn.RegisterMessage(base); err != nil {
				panic("Failed to register bus: " + err.Error())
			}
		}

		m.connections[name] = conn
		m.weights[name] = cfg.Weight
	}

	m.wrr = util.NewWRR(m.weights)
	m.opened = true
}

// Start launches bus subscriptions.
func (m *busModule) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return
	}

	if len(m.connections) == 0 {
		panic("Bus not opened")
	}

	for _, conn := range m.connections {
		if err := conn.Start(); err != nil {
			panic("Failed to start bus: " + err.Error())
		}
	}

	fmt.Printf("infrago bus module is running with %d connections, %d services.\n", len(m.connections), len(m.services))

	m.started = true
}

// Stop terminates bus subscriptions.
func (m *busModule) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return
	}

	for _, conn := range m.connections {
		_ = conn.Stop()
	}

	m.started = false
}

// Close closes bus connections.
func (m *busModule) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.opened {
		return
	}

	for _, conn := range m.connections {
		conn.Close()
	}

	m.connections = make(map[string]Connection, 0)
	m.weights = make(map[string]int, 0)
	m.wrr = nil
	m.opened = false
}

func (m *busModule) subject(prefix, kind, name string) string {
	if prefix == "" {
		return kind + "." + name
	}
	return prefix + kind + "." + name
}

func (m *busModule) subjectBase(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + name
}

func normalizePrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		trimmed = strings.TrimSpace(infra.Identity().Project)
	}
	if trimmed == "" {
		trimmed = infra.INFRAGO
	}
	if !strings.HasSuffix(trimmed, ".") {
		trimmed += "."
	}
	return trimmed
}

func (m *busModule) pick() (Connection, string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.wrr == nil {
		return nil, ""
	}
	name := m.wrr.Next()
	if name == "" {
		return nil, ""
	}
	conn := m.connections[name]
	cfg := m.configs[name]
	return conn, cfg.Prefix
}

// Request sends a request and waits for reply.
func (m *busModule) Request(meta *infra.Meta, name string, value base.Map, timeout time.Duration) (base.Map, base.Res) {
	if err := validateBusName(name); err != nil {
		return nil, infra.ErrorResult(err)
	}

	conn, prefix := m.pick()

	if conn == nil {
		return nil, infra.ErrorResult(errBusNotReady)
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return nil, infra.ErrorResult(err)
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectCall, baseName)
	resBytes, err := conn.Request(subject, data, timeout)
	if err != nil {
		return nil, infra.ErrorResult(err)
	}

	return decodeResponse(resBytes)
}

// Broadcast sends to all subscribers.
func (m *busModule) Broadcast(meta *infra.Meta, name string, value base.Map) error {
	if err := validateBusName(name); err != nil {
		return err
	}

	conn, prefix := m.pick()

	if conn == nil {
		return errBusNotReady
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return err
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectEvent, baseName)
	return conn.Publish(subject, data)
}

// Rolecast sends grouped message: one receiver per role(group).
func (m *busModule) Rolecast(meta *infra.Meta, name string, value base.Map) error {
	if err := validateBusName(name); err != nil {
		return err
	}

	conn, prefix := m.pick()

	if conn == nil {
		return errBusNotReady
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return err
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectGroup, baseName)
	return conn.Enqueue(subject, data)
}

// Dispatch sends async service execution to queue (one subscriber receives).
func (m *busModule) Dispatch(meta *infra.Meta, name string, value base.Map) error {
	if err := validateBusName(name); err != nil {
		return err
	}

	conn, prefix := m.pick()

	if conn == nil {
		return errBusNotReady
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return err
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectQueue, baseName)
	return conn.Enqueue(subject, data)
}

// Publish is compatibility alias of Rolecast.
func (m *busModule) Publish(meta *infra.Meta, name string, value base.Map) error {
	return m.Rolecast(meta, name, value)
}

// Enqueue is compatibility alias of Dispatch.
func (m *busModule) Enqueue(meta *infra.Meta, name string, value base.Map) error {
	return m.Dispatch(meta, name, value)
}

func encodeRequest(meta *infra.Meta, name string, payload base.Map) ([]byte, error) {
	req := busRequest{
		Name:    name,
		Payload: payload,
	}
	if meta != nil {
		req.Metadata = meta.Metadata()
	}
	return msgpack.Marshal(req)
}

func decodeRequest(data []byte) (*infra.Meta, string, base.Map, error) {
	var req busRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return nil, "", nil, err
	}
	if err := validateBusName(req.Name); err != nil {
		return nil, "", nil, err
	}

	meta := infra.NewMeta()
	meta.Metadata(req.Metadata)

	if req.Payload == nil {
		req.Payload = base.Map{}
	}
	return meta, req.Name, req.Payload, nil
}

func validateBusName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("empty bus name")
	}
	if strings.HasPrefix(trimmed, reservedNamePrefix) {
		return errBusReserved
	}
	return nil
}

func encodeResponse(data base.Map, res base.Res) ([]byte, error) {
	if res == nil {
		res = infra.OK
	}
	resp := busResponse{
		Code:   res.Code(),
		Status: res.Status(),
		Result: res.Error(),
		Time:   time.Now().UnixMilli(),
		Data:   data,
	}
	return msgpack.Marshal(resp)
}

func (e DispatchError) Error() string {
	if e.res == nil {
		return "dispatch failed"
	}
	if msg := strings.TrimSpace(e.res.Error()); msg != "" {
		return msg
	}
	status := strings.TrimSpace(e.res.Status())
	if status == "" {
		status = "dispatch failed"
	}
	return status
}

func (e DispatchError) Result() base.Res {
	return e.res
}

func (e DispatchError) Retryable() bool {
	if e.res == nil {
		return false
	}
	if infra.IsRetry(e.res) {
		return true
	}
	switch e.res.Status() {
	case infra.Invalid.Status(), infra.Denied.Status(), infra.Unsigned.Status(), infra.Unauthed.Status():
		return false
	}
	return e.res.Fail()
}

func IsRetryableDispatchError(err error) bool {
	if err == nil {
		return false
	}
	var dispatchErr DispatchError
	if errors.As(err, &dispatchErr) {
		return dispatchErr.Retryable()
	}
	return true
}

func decodeResponse(data []byte) (base.Map, base.Res) {
	var resp busResponse
	if err := msgpack.Unmarshal(data, &resp); err != nil {
		return nil, infra.ErrorResult(err)
	}
	if resp.Status == "" && resp.State != "" {
		resp.Status = resp.State
	}
	if resp.Result == "" && resp.Desc != "" {
		resp.Result = resp.Desc
	}

	res := infra.Result(resp.Code, resp.Status, resp.Result)
	if resp.Data == nil {
		resp.Data = base.Map{}
	}
	return resp.Data, res
}

// HandleCall handles request/reply for a bus instance.
func (inst *Instance) HandleCall(data []byte) ([]byte, error) {
	meta, name, payload, err := decodeRequest(data)
	if err != nil {
		return nil, err
	}

	body, res, _ := host.InvokeLocalService(meta, name, payload)
	return encodeResponse(body, res)
}

// HandleServiceAsync handles async execution (queue) for service entries.
func (inst *Instance) HandleServiceAsync(data []byte, attempt int, final bool) error {
	meta, name, payload, err := decodeRequest(data)
	if err != nil {
		return err
	}

	if attempt <= 0 {
		attempt = 1
	}

	setting := base.Map{
		"_dispatch_attempt": attempt,
		"_dispatch_final":   final,
	}

	_, res, found := host.InvokeLocalService(meta, name, payload, setting)
	if !found {
		return errors.New("service not found: " + name)
	}
	if res != nil && res.Fail() {
		return DispatchError{res: res}
	}
	return nil
}

// HandleMessage handles async execution (broadcast/rolecast) for message entries.
func (inst *Instance) HandleMessage(data []byte) error {
	meta, name, payload, err := decodeRequest(data)
	if err != nil {
		return err
	}

	go host.InvokeLocalMessage(meta, name, payload)
	return nil
}

// HandleAsync is kept as compatibility alias of HandleServiceAsync.
func (inst *Instance) HandleAsync(data []byte) error {
	return inst.HandleServiceAsync(data, 1, false)
}

// Stats returns service statistics from all connections.
func (m *busModule) Stats() []infra.ServiceStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var all []infra.ServiceStats
	for _, conn := range m.connections {
		stats := conn.Stats()
		if stats != nil {
			all = append(all, stats...)
		}
	}
	return all
}

func (m *busModule) ListNodes() []infra.NodeInfo {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	merged := make(map[string]infra.NodeInfo)
	for _, conn := range m.connections {
		nodes := conn.ListNodes()
		for _, item := range nodes {
			key := item.Project + "|" + item.Node + "|" + item.Role + "|" + item.Profile
			current, ok := merged[key]
			if !ok {
				item.Services = uniqueStrings(item.Services)
				merged[key] = item
				continue
			}
			current.Services = mergeStrings(current.Services, item.Services)
			if item.Updated > current.Updated {
				current.Updated = item.Updated
			}
			merged[key] = current
		}
	}

	out := make([]infra.NodeInfo, 0, len(merged))
	for _, item := range merged {
		item.Services = uniqueStrings(item.Services)
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Project == out[j].Project {
			if out[i].Role == out[j].Role {
				if out[i].Profile == out[j].Profile {
					return out[i].Node < out[j].Node
				}
				return out[i].Profile < out[j].Profile
			}
			return out[i].Role < out[j].Role
		}
		return out[i].Project < out[j].Project
	})
	return out
}

func (m *busModule) ListServices() []infra.ServiceInfo {
	nodes := m.ListNodes()
	merged := make(map[string]*infra.ServiceInfo)
	for _, node := range nodes {
		for _, svc := range node.Services {
			svcKey := svc
			info, ok := merged[svcKey]
			if !ok {
				meta := m.services[svc]
				info = &infra.ServiceInfo{
					Service: svc,
					Name:    meta.name,
					Desc:    meta.desc,
					Nodes:   make([]infra.ServiceNode, 0),
				}
				if info.Name == "" {
					info.Name = svc
				}
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
		sort.Slice(info.Nodes, func(i, j int) bool {
			if info.Nodes[i].Role == info.Nodes[j].Role {
				if info.Nodes[i].Profile == info.Nodes[j].Profile {
					return info.Nodes[i].Node < info.Nodes[j].Node
				}
				return info.Nodes[i].Profile < info.Nodes[j].Profile
			}
			return info.Nodes[i].Role < info.Nodes[j].Role
		})
		info.Instances = len(info.Nodes)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Service < out[j].Service
	})
	return out
}

func mergeStrings(a, b []string) []string {
	if len(a) == 0 {
		out := make([]string, len(b))
		copy(out, b)
		return out
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, item := range a {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	for _, item := range b {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func castToMap(value base.Any) (base.Map, bool) {
	switch v := value.(type) {
	case base.Map:
		return v, true
	default:
		return nil, false
	}
}

func parseWeight(value base.Any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func normalizeDurations(in []time.Duration) []time.Duration {
	if len(in) == 0 {
		return nil
	}
	out := make([]time.Duration, 0, len(in))
	for _, item := range in {
		if item > 0 {
			out = append(out, item)
		}
	}
	return out
}

func DispatchFinal(retries []time.Duration, attempt int) bool {
	if len(retries) == 0 {
		return false
	}
	if attempt <= 0 {
		attempt = 1
	}
	return attempt > len(retries)
}

func DispatchRetryDelay(retries []time.Duration, attempt int) (time.Duration, bool) {
	if len(retries) == 0 {
		return 0, false
	}
	if attempt <= 0 {
		attempt = 1
	}
	idx := attempt - 1
	if idx >= len(retries) {
		return 0, false
	}
	delay := retries[idx]
	if delay <= 0 {
		delay = time.Second
	}
	return delay, true
}

func Stats() []infra.ServiceStats {
	return module.Stats()
}

func ListNodes() []infra.NodeInfo {
	return module.ListNodes()
}

func ListServices() []infra.ServiceInfo {
	return module.ListServices()
}
