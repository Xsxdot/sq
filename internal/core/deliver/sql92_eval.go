// sql92_eval.go：SQL92 AST 的三值求值。
//
// 职责：
//   - evalNode：按 SQL 三值逻辑（TRUE/FALSE/UNKNOWN）递归求值
//   - 类型强转：由表达式常量的类型决定如何解释属性字符串
//   - 属性来源映射：Properties 全部键 + TAGS → m.Tag
//
// 边界：
//   - 不打日志：本文件跑在扫描热路径、持队列锁的回调里，逐条打日志会
//     淹掉日志流。该层的可观测性由 deliver 的分桶跳过计数承担
//   - 不返回 error：一条属性格式异常的脏消息不能中断整趟扫描，
//     无法判定一律 ResultUnknown（见 spec §6.1）
package deliver

import (
	"strconv"
	"strings"

	"github.com/xushixin/sq/internal/core"
)

// Match 求值入口：三值求值 AST 根节点。
func (f *SQLFilter) Match(m *core.Message) Result {
	return evalNode(f.root, m)
}

// evalNode 按节点类型分派递归求值。
func evalNode(n node, m *core.Message) Result {
	switch v := n.(type) {
	case *orNode:
		return or3(evalNode(v.left, m), evalNode(v.right, m))
	case *andNode:
		return and3(evalNode(v.left, m), evalNode(v.right, m))
	case *notNode:
		return not3(evalNode(v.inner, m))
	case *cmpNode:
		return evalCmp(v, m)
	case *betweenNode:
		return evalBetween(v, m)
	case *inNode:
		return evalIn(v, m)
	case *isNullNode:
		_, ok := lookupProp(m, v.ident)
		r := ResultTrue
		if ok {
			// 存在即非 NULL；IS NULL 永不返回 UNKNOWN，即便属性值是
			// 无法解释的字符串。
			r = ResultFalse
		}
		if v.negated {
			return not3(r)
		}
		return r
	default:
		// validate 已保证 AST 只含上述节点类型；走到这里说明构造期
		// 校验有漏网，保守判 UNKNOWN（不投递）而不是 panic 打断整趟扫描。
		return ResultUnknown
	}
}

// lookupProp 解析属性来源：TAGS 映射到 m.Tag，其余查 m.Properties。
//
// 为什么 TAGS 优先于同名用户属性：RocketMQ 把 tag 作为可过滤属性暴露，
// sq 的 tag 是消息的独立字段（Message.Tag），不在 Properties 里；若用户
// 恰好定义了同名属性 TAGS，系统映射必须遮蔽它，否则同名两条来源会产生
// 二义（见 spec §4.4）。
//
// 为什么空 tag 算缺失不算空串：属性不存在与属性存在但为空串在 SQL
// 语义里分属 NULL 与 ''，两者行为不同——`TAGS IS NULL` 对空 tag 应为真，
// 空串则应为假（见 spec §4.4）。返回 (v, true) 的调用方都假定"存在即
// 可比较"，把空串混进来会让比较档把空串当真实值。
func lookupProp(m *core.Message, ident string) (string, bool) {
	if ident == "TAGS" {
		if m.Tag == "" {
			return "", false
		}
		return m.Tag, true
	}
	v, ok := m.Properties[ident]
	return v, ok
}

// and3 三值 AND，真值表见 spec §4.1：
// T×T=T, T×F=F, T×U=U, F×{T,F,U}=F, U×T=U, U×F=F, U×U=U。
func and3(a, b Result) Result {
	if a == ResultFalse || b == ResultFalse {
		return ResultFalse
	}
	if a == ResultTrue && b == ResultTrue {
		return ResultTrue
	}
	return ResultUnknown
}

// or3 三值 OR，真值表见 spec §4.1：
// T×{T,F,U}=T, F×T=T, F×F=F, F×U=U, U×T=T, U×F=U, U×U=U。
func or3(a, b Result) Result {
	if a == ResultTrue || b == ResultTrue {
		return ResultTrue
	}
	if a == ResultFalse && b == ResultFalse {
		return ResultFalse
	}
	return ResultUnknown
}

// not3 三值 NOT，真值表见 spec §4.1：T→F, F→T, U→U。
//
// 为什么 not3(ResultUnknown) 必须返回 ResultUnknown：这是三值逻辑最容易
// 退化成二值的一格——属性缺失时 NOT 表达式没有信息可判断，若顺手当成
// TRUE 返回，会把用户明确不想要的属性缺失消息投递出去（见 spec §4.1）。
func not3(a Result) Result {
	switch a {
	case ResultTrue:
		return ResultFalse
	case ResultFalse:
		return ResultTrue
	default:
		return ResultUnknown
	}
}

// evalCmp 比较节点求值：属性缺失直接 UNKNOWN；否则按常量类型分档解释。
func evalCmp(n *cmpNode, m *core.Message) Result {
	prop, ok := lookupProp(m, n.ident)
	if !ok {
		return ResultUnknown
	}
	switch n.val.kind {
	case constString:
		// 大小比较（> >= < <=）已在构建期 validate 拒绝，这里只会是 = <>。
		if (n.val.str == prop) == (n.op == "=") {
			return ResultTrue
		}
		return ResultFalse
	case constBool:
		var b bool
		switch {
		case strings.EqualFold(prop, "true"):
			b = true
		case strings.EqualFold(prop, "false"):
			b = false
		default:
			// 属性值既不是 true 也不是 false，无法按布尔档解释。
			return ResultUnknown
		}
		// 大小比较（> >= < <=）对布尔常量未在构建期拦截，这里按
		// `=`/`<>` 两种朝向判定；其余运算符保守判 UNKNOWN。
		switch n.op {
		case "=":
			if b == n.val.b {
				return ResultTrue
			}
		case "<>":
			if b != n.val.b {
				return ResultTrue
			}
		default:
			return ResultUnknown
		}
		return ResultFalse
	case constNumber:
		return evalNumCmp(n, prop)
	default:
		// constNull 已在构建期拒绝（`x = NULL` 不合法），不可能走到这里。
		return ResultUnknown
	}
}

// evalNumCmp 数值档比较：先试 int64 精确档，失败退 float64 档。
//
// 为什么先试整数档：雪花 ID 等大整数超过 2^53 后 float64 无法表示相邻
// 整数，纯 float64 会把 9007199254740992 与 9007199254740993 判为相等——
// 静默的错误匹配。常量侧 isInt/i64 在解析期就保住了精确值，这里只要
// 属性也能 ParseInt 成功就走 int64 直比。有一致性看门用例守住该行为。
func evalNumCmp(n *cmpNode, prop string) Result {
	if n.val.isInt {
		if p, err := strconv.ParseInt(prop, 10, 64); err == nil {
			// cmpNode 是 ident op const 朝向，属性值在运算符左侧。
			return cmpInt(n.op, p, n.val.i64)
		}
	}
	// 常量 isInt=true 但属性解析不成 int64（如 "3.5"），或常量本身是
	// 小数：统一退 float64 档，常量 num 与属性 ParseFloat 后比较。
	pv, err := strconv.ParseFloat(prop, 64)
	if err != nil {
		return ResultUnknown
	}
	return cmpFloat(n.op, pv, n.val.num)
}

// cmpInt int64 档精确比较。
func cmpInt(op string, lhs, rhs int64) Result {
	switch op {
	case "=":
		if lhs == rhs {
			return ResultTrue
		}
	case "<>":
		if lhs != rhs {
			return ResultTrue
		}
	case ">":
		if lhs > rhs {
			return ResultTrue
		}
	case ">=":
		if lhs >= rhs {
			return ResultTrue
		}
	case "<":
		if lhs < rhs {
			return ResultTrue
		}
	case "<=":
		if lhs <= rhs {
			return ResultTrue
		}
	}
	return ResultFalse
}

// cmpFloat float64 档比较。
func cmpFloat(op string, lhs, rhs float64) Result {
	switch op {
	case "=":
		if lhs == rhs {
			return ResultTrue
		}
	case "<>":
		if lhs != rhs {
			return ResultTrue
		}
	case ">":
		if lhs > rhs {
			return ResultTrue
		}
	case ">=":
		if lhs >= rhs {
			return ResultTrue
		}
	case "<":
		if lhs < rhs {
			return ResultTrue
		}
	case "<=":
		if lhs <= rhs {
			return ResultTrue
		}
	}
	return ResultFalse
}

// evalBetween BETWEEN 求值：按展开式 and3(ge, le) 实现，保证与
// `a >= lo AND a <= hi` 逐格等价（含三值传播）；negated 时套 not3。
func evalBetween(n *betweenNode, m *core.Message) Result {
	// 为什么这里不走 evalCmp：展开式 `a >= lo` 对字符串常量本就被构建期
	// validate 拒绝（字符串不支持大小比较），逐字等价无从谈起；数值档的
	// BETWEEN 才是有意义的用法，其他类型保守判 UNKNOWN。
	if n.lo.kind != constNumber {
		return ResultUnknown
	}
	prop, ok := lookupProp(m, n.ident)
	if !ok {
		return ResultUnknown
	}
	ge := evalNumCmp(&cmpNode{ident: n.ident, op: ">=", val: n.lo}, prop)
	le := evalNumCmp(&cmpNode{ident: n.ident, op: "<=", val: n.hi}, prop)
	r := and3(ge, le)
	if n.negated {
		return not3(r)
	}
	return r
}

// evalIn IN 求值：逐个构造 `a = val` 用 or3 折叠，保证与展开式逐格等价；
// negated 时套 not3。
func evalIn(n *inNode, m *core.Message) Result {
	// 零元素 IN 已被解析器拒绝（parseIn 要求至少一个元素）。
	res := ResultFalse
	for _, v := range n.vals {
		eq := evalCmp(&cmpNode{ident: n.ident, op: "=", val: v}, m)
		res = or3(res, eq)
	}
	if n.negated {
		return not3(res)
	}
	return res
}
