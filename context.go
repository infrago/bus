package bus

import (
	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Context struct {
		inst *Instance
		infra.Meta

		index int       //下一个索引
		nexts []ctxFunc //方法列表

		// 以下几个字段必须独立
		// 要不然，Invoke的时候，会被修改掉
		Name    string
		Config  *infra.Service
		Value   Map
		Setting Map

		Type string
		Body Map

		callback Callback
	}
	ctxFunc func(*Context)
)

func (ctx *Context) clear() {
	ctx.index = 0
	ctx.nexts = make([]ctxFunc, 0)
}
func (ctx *Context) next(nexts ...ctxFunc) {
	ctx.nexts = append(ctx.nexts, nexts...)
}

func (ctx *Context) Next() {
	if len(ctx.nexts) > ctx.index {
		next := ctx.nexts[ctx.index]
		ctx.index++
		if next != nil {
			next(ctx)
		} else {
			ctx.Next()
		}
	}
}

func (ctx *Context) Found() {
	ctx.inst.found(ctx)
}
func (ctx *Context) Erred(res Res) {
	ctx.Result(res)
	ctx.inst.error(ctx)
}
func (ctx *Context) Failed(res Res) {
	ctx.Result(res)
	ctx.inst.failed(ctx)
}
func (ctx *Context) Denied(res Res) {
	ctx.Result(res)
	ctx.inst.denied(ctx)
}
