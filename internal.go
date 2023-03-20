package bus

import (
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

// 请求
func (this *Module) request(meta infra.Metadata, timeout time.Duration) (*infra.Echo, error) {
	locate := this.hashring.Locate(meta.Name)

	if inst, ok := this.instances[locate]; ok {
		//编码数据
		reqBytes, err := infra.Marshal(inst.Config.Codec, &meta)
		if err != nil {
			return nil, err
		}

		realName := inst.Config.Prefix + meta.Name
		resBytes, err := inst.connect.Request(realName, reqBytes, timeout)
		if err != nil {
			return nil, err
		}

		echo := &infra.Echo{Code: infra.OK.Code(), Text: infra.OK.Error()}

		//解码返回的数据
		err = infra.Unmarshal(inst.Config.Codec, resBytes, echo)
		if err != nil {
			return nil, err
		}

		// 数据处理，当gob的时候，空数组，会变成nil值，
		// JSON输出的时候就不是[]而是nil了，要处理一下
		//数据处理，因为gob转换后，[]Map为变成一个带类型的nil值，前端可能会得到nil，而不是[]
		//要递归处理
		this.handleNullData(echo.Data)

		return echo, nil
	}

	return nil, errInvalidConnection
}

func (this *Module) handleNullData(data Map) {
	for k, v := range data {
		switch vs := v.(type) {
		case Map:
			this.handleNullData(vs)
		case []Any:
			if vs == nil || len(vs) == 0 {
				data[k] = []Any{}
			} else {
				for _, v := range vs {
					if vm, ok := v.(Map); ok {
						this.handleNullData(vm)
					}
				}
			}
		case []Map:
			if vs == nil || len(vs) == 0 {
				data[k] = []Map{}
			} else {
				for _, v := range vs {
					this.handleNullData(v)
				}
			}
		case []string:
			if vs == nil || len(vs) == 0 {
				data[k] = []string{}
			}
		case []int:
			if vs == nil || len(vs) == 0 {
				data[k] = []int{}
			}
		case []int8:
			if vs == nil || len(vs) == 0 {
				data[k] = []int8{}
			}
		case []int16:
			if vs == nil || len(vs) == 0 {
				data[k] = []int16{}
			}
		case []int32:
			if vs == nil || len(vs) == 0 {
				data[k] = []int32{}
			}
		case []int64:
			if vs == nil || len(vs) == 0 {
				data[k] = []int64{}
			}
		case []float32:
			if vs == nil || len(vs) == 0 {
				data[k] = []float32{}
			}
		case []float64:
			if vs == nil || len(vs) == 0 {
				data[k] = []float64{}
			}
		case []bool:
			if vs == nil || len(vs) == 0 {
				data[k] = []bool{}
			}
		}
	}
}

func (this *Module) Request(meta infra.Metadata, timeout time.Duration) (*infra.Echo, error) {
	return this.request(meta, timeout)
}
