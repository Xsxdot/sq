// sql92_parse.go：SQL92 过滤表达式的语法解析（token 流 → AST）与构建期语义校验。
//
// 职责：
//   - 按 spec §3.1 文法递归下降：OR < AND < NOT < primary
//   - 产出带语义信息的最小 AST（比较、BETWEEN、IN、IS NULL、NOT、AND、OR）
//   - buildSQLFilter 在 AST 上做构建期语义校验（类型混用、NULL 比较、深度/长度/元素数上限）
//
// 边界：
//   - 语法错误带 1-based 列号，原样进入 ILLEGAL_FILTER_EXPRESSION 回给客户端；
//     构建期语义错误是表达式层面的，无列号，仅带子串可辨的原因
//   - 常量在左的比较在此归一化为 ident 在左并翻转运算符，求值层只处理一种朝向
package deliver

import (
	"fmt"
	"strconv"

	"github.com/xushixin/sq/internal/core"
)

// node 语法树节点的公共接口，所有具体节点类型实现空的 isNode()。
type node interface{ isNode() }

// orNode OR 表达式：left OR right，左右均为布尔子表达式。
type orNode struct{ left, right node }

func (*orNode) isNode() {}

// andNode AND 表达式：left AND right，左右均为布尔子表达式。
type andNode struct{ left, right node }

func (*andNode) isNode() {}

// notNode NOT 表达式：NOT inner。前缀 NOT（inner 之前的 NOT）。
type notNode struct{ inner node }

func (*notNode) isNode() {}

// cmpNode 比较表达式：ident op val。
// identOnLeft 为 true 表示原文就是属性名在左；为 false 表示常量在左、
// 解析器已翻转运算符归一化，仅供错误信息按原文还原用。
type cmpNode struct {
	ident       string
	op          string
	val         constant
	identOnLeft bool
}

func (*cmpNode) isNode() {}

// betweenNode BETWEEN 表达式：ident [NOT] BETWEEN lo AND hi。
// negated 对应 BETWEEN 前的 NOT。
type betweenNode struct {
	ident   string
	lo, hi  constant
	negated bool
}

func (*betweenNode) isNode() {}

// inNode IN 表达式：ident [NOT] IN (vals...)。
// negated 对应 IN 前的 NOT。
type inNode struct {
	ident   string
	vals    []constant
	negated bool
}

func (*inNode) isNode() {}

// isNullNode IS [NOT] NULL 表达式。negated 对应 IS 与 NULL 之间的 NOT。
type isNullNode struct {
	ident   string
	negated bool
}

func (*isNullNode) isNode() {}

// constKind 常量的类别。
type constKind uint8

const (
	// constNumber 数值常量。
	constNumber constKind = iota
	// constString 字符串常量，str 存原文（已去引号、完成转义）。
	constString
	// constBool 布尔常量 TRUE/FALSE，b 存值。
	constBool
	// constNull NULL 常量，专用于 `a = NULL` 这类显式 NULL 比较。
	constNull
)

// constant 一个常量值。
//
// 字段说明：
//   - kind: 四类之一（constNumber/constString/constBool/constNull）
//   - str: 字符串常量原文
//   - num: float64 数值，普通浮点比较统一走这里
//   - i64: int64 数值，isInt 为 true 时有效，供精确整数比较
//   - isInt: 原文能否精确按十进制整数解析（ParseInt 成功）
//   - b: 布尔值
//
// 为什么 num 与 i64 并存：雪花 ID 等大整数超过 2^53 后 float64 在解析期
// 就丢精度，纯 float64 会把 9007199254740993 存成 9007199254740992；
// isInt/i64 保留精确值供 Task 5 做整数比较，num 仍填同值，普通比较路径
// 不需要对整数特判。
type constant struct {
	kind  constKind
	str   string
	num   float64
	i64   int64
	isInt bool
	b     bool
}

// parser 递归下降解析器：持有 token 流与当前位置。
type parser struct {
	toks  []token
	pos   int
	depth int // 括号嵌套深度，防止病态深括号打爆递归
}

// parseSQL92 把过滤表达式解析为 AST 根节点。
func parseSQL92(expr string) (node, error) {
	toks, err := lex(expr)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, p.errorf(p.peek(), "期望表达式结束，实际读到 %s", tokDesc(p.peek()))
	}
	return n, nil
}

const (
	// maxExprBytes 表达式长度上限。远高于任何正常表达式，卡的是病态输入。
	// 不做配置项：spec §2.3 决定用上限替代配置开关，阈值一旦可配就等于
	// 把开关又加了回来。
	maxExprBytes = 1024
	// maxASTDepth AST 嵌套深度上限，防深层递归求值。
	maxASTDepth = 16
	// maxInElems 单个 IN 列表的元素数上限。
	maxInElems = 64
)

// SQLFilter SQL92 过滤表达式：已通过构建期校验的 AST + 求值入口。
//
// 实现 Filter 接口（见 filter.go）；构造必须走 buildSQLFilter，直接拼
// SQLFilter{} 会绕过语义校验。
type SQLFilter struct {
	root node
}

// Match 求值入口。Task 5 实现真正的三值求值；此刻返回 ResultUnknown 是
// 保守的占位——UNKNOWN 不投递，接错线也不会把不该投的消息投出去。
func (f *SQLFilter) Match(m *core.Message) Result { return ResultUnknown }

// buildSQLFilter 构建一个 SQL92 过滤器：先做长度上限检查，再语法解析，
// 最后在 AST 上做构建期语义校验。任一步失败都返回带原因的 error。
func buildSQLFilter(expr string) (*SQLFilter, error) {
	if len(expr) > maxExprBytes {
		return nil, fmt.Errorf("过滤表达式长度 %d 超过上限 %d 字节", len(expr), maxExprBytes)
	}
	root, err := parseSQL92(expr)
	if err != nil {
		return nil, err
	}
	if err := validate(root, 1); err != nil {
		return nil, err
	}
	return &SQLFilter{root: root}, nil
}

// validate 构建期语义校验，按节点类型检查：
//   - 任意节点：嵌套深度超过 maxASTDepth
//   - cmpNode：字符串常量不得用于 > >= < <=；NULL 只允许出现在 IS NULL
//   - inNode：元素数不超过 maxInElems，且元素类型一致
//   - betweenNode：上下界类型一致
//
// 错误不带列号：语法层面已带列号，语义层面是整棵子树的属性，定位到列
// 反而误导。
func validate(n node, depth int) error {
	if depth > maxASTDepth {
		return fmt.Errorf("表达式嵌套深度超过上限 %d 层", maxASTDepth)
	}
	switch v := n.(type) {
	case *orNode:
		if err := validate(v.left, depth+1); err != nil {
			return err
		}
		return validate(v.right, depth+1)
	case *andNode:
		if err := validate(v.left, depth+1); err != nil {
			return err
		}
		return validate(v.right, depth+1)
	case *notNode:
		return validate(v.inner, depth+1)
	case *cmpNode:
		if v.val.kind == constString {
			switch v.op {
			case ">", ">=", "<", "<=":
				// 字符串只支持相等性判断（= <> IN），大小比较无字典序
				// 依据，spec §4.2 明确排除。
				return fmt.Errorf("字符串常量不支持大小比较（仅 = <> IN）")
			}
		}
		if v.val.kind == constNull {
			// = NULL 恒 UNKNOWN（NULL 不参与相等比较），SQL 标准里合法但
			// 对过滤毫无意义，直接拒绝并给出可照做的替代写法。
			return fmt.Errorf("%s = NULL 恒不成立，请改用 %s IS NULL", v.ident, v.ident)
		}
	case *inNode:
		if len(v.vals) > maxInElems {
			return fmt.Errorf("IN 列表元素数 %d 超过上限 %d", len(v.vals), maxInElems)
		}
		for i := 1; i < len(v.vals); i++ {
			if v.vals[i].kind != v.vals[0].kind {
				return fmt.Errorf("IN 列表的常量类型必须一致")
			}
		}
	case *betweenNode:
		if v.lo.kind != v.hi.kind {
			return fmt.Errorf("BETWEEN 上下界的常量类型必须一致")
		}
	}
	return nil
}

// parseOr 文法：andExpr { OR andExpr }。优先级最低。
func (p *parser) parseOr() (node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokKeyword && p.peek().text == "OR" {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &orNode{left: left, right: right}
	}
	return left, nil
}

// parseAnd 文法：notExpr { AND notExpr }。
func (p *parser) parseAnd() (node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokKeyword && p.peek().text == "AND" {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &andNode{left: left, right: right}
	}
	return left, nil
}

// parseNot 文法：[ NOT ] primary。这里只管 primary 之前的 NOT；
// `a NOT IN (...)` / `a NOT BETWEEN ...` 的 NOT 在属性名之后，由
// parseIdentForm 识别并置 negated，两者不冲突。
func (p *parser) parseNot() (node, error) {
	if p.peek().kind == tokKeyword && p.peek().text == "NOT" {
		p.next()
		inner, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &notNode{inner: inner}, nil
	}
	return p.parsePrimary()
}

// parsePrimary 文法：括号表达式 | 属性名形式 | 常量在左的比较。
func (p *parser) parsePrimary() (node, error) {
	t := p.peek()
	switch t.kind {
	case tokLParen:
		// 为什么在解析层卡括号深度：括号在 AST 里是透明的（parsePrimary
		// 直接返回 inner），validate 只看得见节点嵌套，看不见括号层数；
		// 若不在此拦截，`((((...a = 1...))))` 会绕过 validate 的深度检查，
		// 递归解析器本身也会随括号数线性下探。
		if p.depth >= maxASTDepth {
			return nil, p.errorf(t, "表达式嵌套深度超过上限 %d 层", maxASTDepth)
		}
		p.depth++
		p.next()
		inner, err := p.parseOr()
		p.depth--
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tokRParen {
			return nil, p.errorf(p.peek(), "期望 ')' 关闭括号表达式，实际读到 %s", tokDesc(p.peek()))
		}
		p.next()
		return inner, nil
	case tokIdent:
		return p.parseIdentForm()
	case tokNumber, tokString:
		return p.parseConstLeft()
	case tokKeyword:
		if t.text == "TRUE" || t.text == "FALSE" {
			return p.parseConstLeft()
		}
	}
	return nil, p.errorf(t, "期望比较、BETWEEN、IN、IS NULL 或括号表达式，实际读到 %s", tokDesc(t))
}

// parseIdentForm 属性名开头的谓词：消费属性名后按下一个 token 分派到
// 比较 / IS NULL / [NOT] IN / [NOT] BETWEEN。
func (p *parser) parseIdentForm() (node, error) {
	ident := p.next().text // 消费属性名，ident 即其原文
	nxt := p.peek()
	switch nxt.kind {
	case tokOp:
		op := p.next()
		// 属性对属性（`a = b`）在文法层被拒：AST 的 cmpNode 只支持
		// ident 对常量，右侧必须是常量。
		if p.peek().kind == tokIdent {
			return nil, p.errorf(p.peek(), "比较两侧至少要有一个属性名")
		}
		val, err := p.parseConstant()
		if err != nil {
			return nil, err
		}
		return &cmpNode{ident: ident, op: op.text, val: val, identOnLeft: true}, nil
	case tokKeyword:
		switch nxt.text {
		case "IS":
			return p.parseIsNull(ident) // parseIsNull 自己消费 IS
		case "NOT":
			return p.parseNotInBetween(ident) // parseNotInBetween 自己消费 NOT
		case "BETWEEN":
			p.next()
			return p.parseBetween(ident, false)
		case "IN":
			p.next()
			return p.parseIn(ident, false)
		}
	}
	return nil, p.errorf(nxt, "期望比较运算符、BETWEEN、IN 或 IS NULL，实际读到 %s", tokDesc(nxt))
}

// parseIsNull 处理 `ident IS [NOT] NULL`。IS 已被消费。
func (p *parser) parseIsNull(ident string) (node, error) {
	p.next() // 消费 IS
	negated := false
	if p.peek().kind == tokKeyword && p.peek().text == "NOT" {
		p.next()
		negated = true
	}
	if !(p.peek().kind == tokKeyword && p.peek().text == "NULL") {
		return nil, p.errorf(p.peek(), "IS 之后期望 [NOT] NULL，实际读到 %s", tokDesc(p.peek()))
	}
	p.next()
	return &isNullNode{ident: ident, negated: negated}, nil
}

// parseNotInBetween 处理属性名后的 NOT：`ident NOT IN` / `ident NOT BETWEEN`。
// NOT 已被消费，其后必须是 IN 或 BETWEEN，否则不构成合法谓词。
func (p *parser) parseNotInBetween(ident string) (node, error) {
	p.next() // 消费 NOT
	nxt := p.peek()
	switch {
	case nxt.kind == tokKeyword && nxt.text == "IN":
		p.next()
		return p.parseIn(ident, true)
	case nxt.kind == tokKeyword && nxt.text == "BETWEEN":
		p.next()
		return p.parseBetween(ident, true)
	}
	return nil, p.errorf(nxt, "属性名后的 NOT 只允许出现在 NOT IN / NOT BETWEEN，实际读到 %s", tokDesc(nxt))
}

// parseBetween 处理 BETWEEN 谓词：下界 AND 上界，BETWEEN 已被消费。
func (p *parser) parseBetween(ident string, negated bool) (node, error) {
	lo, err := p.parseConstant()
	if err != nil {
		return nil, err
	}
	// 为什么：BETWEEN 的 AND 必须由本分支自己消费。
	// 文法上 `a BETWEEN 1 AND 2` 的 AND 是 BETWEEN 谓词的一部分，不是
	// 布尔连接词；若留给 andExpr 层处理，`a BETWEEN 1 AND 2 AND b = 3`
	// 会被拆成 between(a, 1, and(...)) 或直接解析失败。这里显式 expect
	// 掉分隔 AND，上层 andExpr 只能看到 BETWEEN 之外的 AND。
	if !(p.peek().kind == tokKeyword && p.peek().text == "AND") {
		return nil, p.errorf(p.peek(), "BETWEEN 上下界之间期望 AND，实际读到 %s", tokDesc(p.peek()))
	}
	p.next()
	hi, err := p.parseConstant()
	if err != nil {
		return nil, err
	}
	return &betweenNode{ident: ident, lo: lo, hi: hi, negated: negated}, nil
}

// parseIn 处理 IN 谓词：(e1, e2, ...)，IN 已被消费。
func (p *parser) parseIn(ident string, negated bool) (node, error) {
	if p.peek().kind != tokLParen {
		return nil, p.errorf(p.peek(), "IN 之后期望 '('，实际读到 %s", tokDesc(p.peek()))
	}
	p.next()
	var vals []constant
	if p.peek().kind == tokRParen {
		return nil, p.errorf(p.peek(), "IN 列表至少需要一个元素")
	}
	for {
		v, err := p.parseConstant()
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
		switch p.peek().kind {
		case tokComma:
			p.next()
		case tokRParen:
			p.next()
			return &inNode{ident: ident, vals: vals, negated: negated}, nil
		default:
			return nil, p.errorf(p.peek(), "IN 列表期望 ',' 或 ')'，实际读到 %s", tokDesc(p.peek()))
		}
	}
}

// parseConstLeft 常量在左的比较：`<常量> <op> <属性名>`，归一化为
// 属性名在左的 cmpNode 并翻转运算符。
func (p *parser) parseConstLeft() (node, error) {
	val, err := p.parseConstant()
	if err != nil {
		return nil, err
	}
	nxt := p.peek()
	if nxt.kind == tokOp {
		op := p.next()
		// 常量在左时右侧必须是属性名，常量对常量（如 `1 = 1`）在文法层拒绝。
		if p.peek().kind != tokIdent {
			return nil, p.errorf(p.peek(), "比较两侧至少要有一个属性名")
		}
		ident := p.next()
		// 为什么：常量在左必须归一化为属性名在左并翻转运算符。
		// 求值层只需处理 ident 在左一种朝向，比较逻辑不用按朝向分叉；
		// identOnLeft=false 仅用于错误信息按原文还原。
		return &cmpNode{ident: ident.text, op: flipOp(op.text), val: val, identOnLeft: false}, nil
	}
	if nxt.kind == tokKeyword && (nxt.text == "IS" || nxt.text == "BETWEEN" || nxt.text == "IN") {
		return nil, p.errorf(nxt, "IS NULL/BETWEEN/IN 左侧必须是属性名")
	}
	return nil, p.errorf(nxt, "期望比较运算符，实际读到 %s", tokDesc(nxt))
}

// parseConstant 读一个常量：数值 / 字符串 / TRUE / FALSE / NULL。
func (p *parser) parseConstant() (constant, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		// 先试十进制整数，成功则 isInt=true、同时填 i64 与 num；
		// 失败（小数、科学计数法外的浮点写法）再退回 ParseFloat。
		// 为什么：雪花 ID 等大整数超过 2^53 后 float64 解析期就丢精度，
		// isInt/i64 保住精确值，Task 5 的精确整数比较依赖它。
		if i, err := strconv.ParseInt(t.text, 10, 64); err == nil {
			p.next()
			return constant{kind: constNumber, num: float64(i), i64: i, isInt: true}, nil
		}
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return constant{}, p.errorf(t, "非法数值常量 %q", t.text)
		}
		p.next()
		return constant{kind: constNumber, num: f}, nil
	case tokString:
		p.next()
		return constant{kind: constString, str: t.text}, nil
	case tokKeyword:
		switch t.text {
		case "TRUE":
			p.next()
			return constant{kind: constBool, b: true}, nil
		case "FALSE":
			p.next()
			return constant{kind: constBool, b: false}, nil
		case "NULL":
			p.next()
			return constant{kind: constNull}, nil
		}
	}
	return constant{}, p.errorf(t, "期望常量，实际读到 %s", tokDesc(t))
}

// flipOp 常量在左时把运算符翻转到等价形式：`10 < age` 即 `age > 10`。
// 等于与不等于左右对称，无需翻转。
func flipOp(op string) string {
	switch op {
	case "<":
		return ">"
	case ">":
		return "<"
	case "<=":
		return ">="
	case ">=":
		return "<="
	default:
		return op
	}
}

// peek 返回当前 token（不移动）。
func (p *parser) peek() token {
	return p.toks[p.pos]
}

// next 消费并返回当前 token。
func (p *parser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

// errorf 构造带 1-based 列号的语法错误。
func (p *parser) errorf(tok token, format string, args ...any) error {
	return fmt.Errorf("第 %d 列："+format, append([]any{tok.col}, args...)...)
}

// tokDesc 把 token 描述成可读文本，EOF 显示为「表达式结束」而非空串。
func tokDesc(t token) string {
	if t.kind == tokEOF {
		return "表达式结束"
	}
	return strconv.Quote(t.text)
}
