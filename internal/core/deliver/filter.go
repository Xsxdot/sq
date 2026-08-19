// filter.go：服务端订阅过滤的统一入口与 TAG 实现。
//
// 职责：
//   - Result 三值与 Filter 接口：两种过滤器（TAG / SQL92）的共同形状
//   - AllPass 单例：不过滤的唯一表示
//   - ParseFilter：按种类分派到具体解析器
//   - TagFilter：RocketMQ TAG 表达式子集（"*" / "tagA" / "tagA || tagB"）
//
// 边界：
//   - 不做协议枚举映射：FilterKind 是 deliver 自己的类型，pb.FilterType
//     的翻译在 internal/rpc 完成（core 不 import pb）
//   - 过滤的位点语义（跳过即永久越过位点）由 deliver 主流程负责，
//     本文件只管匹配
//   - SQL92 的词法/语法/求值在 sql92_*.go，本文件只留分派
package deliver

import (
	"fmt"
	"strings"

	"github.com/Xsxdot/sq/internal/core"
)

// Result 过滤判定的三值结果。
//
// 为什么不是 bool：调用方需要把"明确不匹配"和"无法判定"分开计数——
// 前者说明表达式对但没有匹配的消息，后者说明属性名拼错或类型对不上，
// 两者的排查方向完全相反（见 spec §7.1）。bool 会把它们压成同一个值。
type Result uint8

const (
	// ResultTrue 命中，投递。
	ResultTrue Result = iota
	// ResultFalse 明确不命中。
	ResultFalse
	// ResultUnknown 无法判定：属性缺失，或属性值无法按表达式常量的类型解释。
	// 投递决策上等同 ResultFalse（都不投），但必须单独计数。
	ResultUnknown
)

// String 便于测试失败信息与日志阅读。
func (r Result) String() string {
	switch r {
	case ResultTrue:
		return "true"
	case ResultFalse:
		return "false"
	case ResultUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("Result(%d)", uint8(r))
	}
}

// Filter 服务端订阅过滤器。
//
// 实现必须不可变：同一实例在一次 ReceiveMessage 内跨消息复用，且可能被
// 并发读取（本包不对过滤器加锁）。
type Filter interface {
	// Match 判定消息是否命中。只有 ResultTrue 才投递。
	// 实现不得返回 error：求值跑在持队列锁的扫描回调里，一条脏消息
	// 不能中断整趟扫描（见 spec §6.1）。无法判定一律 ResultUnknown。
	Match(m *core.Message) Result
}

// FilterKind 过滤表达式的种类。deliver 自己的枚举，与 pb.FilterType 无关。
type FilterKind uint8

const (
	// FilterTag RocketMQ TAG 表达式。
	FilterTag FilterKind = iota
	// FilterSQL92 RocketMQ SQL92 属性过滤表达式。
	FilterSQL92
)

// allPass 匹配一切的过滤器。
type allPass struct{}

func (allPass) Match(*core.Message) Result { return ResultTrue }

// AllPass 不过滤的唯一表示。
//
// 为什么要有这个单例而不是用 nil *TagFilter：接口化之后 nil 具体指针赋给
// Filter 会变成 typed-nil（`f != nil` 为真，方法调用还得靠 nil receiver
// 兜底）。这种写法能跑，但加入第二个实现时极易踩中。用单例把 nil 依赖
// 整个删掉，deliver 因此永远拿不到 nil Filter。
var AllPass Filter = allPass{}

// ParseFilter 按种类解析过滤表达式。
//
// 参数：
//   - kind: 过滤种类，由协议层从 pb.FilterType 映射而来
//   - expr: 表达式原文
//
// 返回：
//   - FilterTag 的"*"、空串、纯空白一律返回 AllPass 单例
//   - 表达式非法或种类不支持时返回 error，错误串带足够定位信息，
//     调用方原样回给客户端
//
// 种类校验必须先于 AllPass 捷径：未知 kind 即使是"*"也必须报错——它是
// 协议枚举映射漏了分支的编程错误，静默当成不过滤会把本应筛掉的消息全量放出。
func ParseFilter(kind FilterKind, expr string) (Filter, error) {
	switch kind {
	case FilterTag:
		if strings.TrimSpace(expr) == "" || strings.TrimSpace(expr) == "*" {
			return AllPass, nil
		}
		return parseTagFilter(expr)
	case FilterSQL92:
		return buildSQLFilter(expr)
	default:
		return nil, fmt.Errorf("未知过滤种类 %d", uint8(kind))
	}
}

// TagFilter 不可变 tag 集合过滤器。
type TagFilter struct {
	tags map[string]struct{}
}

// parseTagFilter 解析 TAG 表达式，按 "||" 分隔为 tag 集合。
// "*" 与空串已由 ParseFilter 提前拦成 AllPass，不会走到这里。
func parseTagFilter(expr string) (*TagFilter, error) {
	set := map[string]struct{}{}
	for _, tok := range strings.Split(expr, "||") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			return nil, fmt.Errorf("TAG 表达式含空 tag: %q", expr)
		}
		set[tok] = struct{}{}
	}
	return &TagFilter{tags: set}, nil
}

// Match 判定 tag 是否命中。区分大小写（与 RocketMQ 一致）。
// 永不返回 ResultUnknown：tag 是消息固有字段，不存在无法判定的情形。
func (f *TagFilter) Match(m *core.Message) Result {
	if _, ok := f.tags[m.Tag]; ok {
		return ResultTrue
	}
	return ResultFalse
}
