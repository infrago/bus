package bus

import (
	"time"

	"github.com/infrago/infra"
)

func init() {
	infra.Register(bridge)
}

var (
	bridge = &Bridge{}
)

type (
	Bridge struct{}
)

func (this *Bridge) Request(meta infra.Metadata, timeout time.Duration) (*infra.Echo, error) {
	return module.Request(meta, timeout)
}
