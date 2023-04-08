package bus

import "time"

type (
	// Driver 数据驱动
	Driver interface {
		Connect(*Instance) (Connect, error)
	}
	Health struct {
		Workload int64
	}

	// Connect 连接
	Connect interface {
		Open() error
		Health() (Health, error)
		Close() error

		Register(name string) error

		Start() error
		Stop() error

		Request(name string, data []byte, timeout time.Duration) ([]byte, error)
	}

	Callback = func([]byte, error)
)
