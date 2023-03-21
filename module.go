package bus

import (
	"sync"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"

	"github.com/panjf2000/ants/v2"
)

func init() {
	infra.Mount(module)
}

var (
	module = &Module{
		configs:   make(map[string]Config, 0),
		drivers:   make(map[string]Driver, 0),
		instances: make(map[string]*Instance, 0),

		services: make(map[string]infra.Service, 0),
		filters:  make(map[string]Filter, 0),
		handlers: make(map[string]Handler, 0),

		relates: make(map[string]string, 0),
	}
)

type (
	Module struct {
		mutex sync.Mutex
		pool  *ants.Pool

		connected, initialized, launched bool

		configs map[string]Config
		drivers map[string]Driver

		services map[string]infra.Service
		filters  map[string]Filter
		handlers map[string]Handler

		relates map[string]string

		requestFilters  []ctxFunc
		executeFilters  []ctxFunc
		responseFilters []ctxFunc

		foundHandlers  []ctxFunc
		errorHandlers  []ctxFunc
		failedHandlers []ctxFunc
		deniedHandlers []ctxFunc

		instances map[string]*Instance

		weights  map[string]int
		hashring *util.HashRing
	}
	Instance struct {
		module  *Module
		Name    string
		Config  Config
		connect Connect
	}

	Config struct {
		Driver  string
		Codec   string
		Weight  int
		Prefix  string
		Setting Map
	}
	Configs map[string]Config

	// Filter 拦截器
	Filter struct {
		Name     string  `json:"name"`
		Text     string  `json:"text"`
		Request  ctxFunc `json:"-"`
		Execute  ctxFunc `json:"-"`
		Response ctxFunc `json:"-"`
	}
	// Handler 处理器
	Handler struct {
		Name   string  `json:"name"`
		Text   string  `json:"text"`
		Found  ctxFunc `json:"-"`
		Error  ctxFunc `json:"-"`
		Failed ctxFunc `json:"-"`
		Denied ctxFunc `json:"-"`
	}
)

// Driver 注册驱动
func (module *Module) Driver(name string, driver Driver) {
	module.mutex.Lock()
	defer module.mutex.Unlock()

	if driver == nil {
		panic("Invalid bus driver: " + name)
	}

	if infra.Override() {
		module.drivers[name] = driver
	} else {
		if module.drivers[name] == nil {
			module.drivers[name] = driver
		}
	}
}

func (this *Module) Config(name string, config Config) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}

	if infra.Override() {
		this.configs[name] = config
	} else {
		if _, ok := this.configs[name]; ok == false {
			this.configs[name] = config
		}
	}
}
func (this *Module) Configs(config Configs) {
	for key, val := range config {
		this.Config(key, val)
	}
}

func (module *Module) Service(name string, config infra.Service) {
	module.mutex.Lock()
	defer module.mutex.Unlock()

	alias := make([]string, 0)
	if name != "" {
		alias = append(alias, name)
	}
	if config.Alias != nil {
		alias = append(alias, config.Alias...)
	}

	for _, key := range alias {
		if infra.Override() {
			module.services[key] = config
		} else {
			if _, ok := module.services[key]; ok == false {
				module.services[key] = config
			}
		}
	}
}

// Filter 注册 拦截器
func (module *Module) Filter(name string, config Filter) {
	if infra.Override() {
		module.filters[name] = config
	} else {
		if _, ok := module.filters[name]; ok == false {
			module.filters[name] = config
		}
	}
}

// Handler 注册 处理器
func (module *Module) Handler(name string, config Handler) {
	if infra.Override() {
		module.handlers[name] = config
	} else {
		if _, ok := module.handlers[name]; ok == false {
			module.handlers[name] = config
		}
	}
}
