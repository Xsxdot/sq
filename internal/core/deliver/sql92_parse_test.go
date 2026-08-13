// sql92_parse_test.go：语法解析器行为契约测试。
//
// 职责：
//   - 锁定 parseSQL92 的优先级、节点形状、常量归一化与错误形态
//
// 边界：
//   - 只测语法层，不测求值（那是 sql92_eval 的事）
package deliver

import (
	"strconv"
	"strings"
	"testing"
)

func TestParseSQL92Precedence(t *testing.T) {
	// NOT > AND > OR。a = 1 OR b = 2 AND c = 3 必须是
	// or(cmp(a,1), and(cmp(b,2), cmp(c,3)))
	n, err := parseSQL92("a = 1 OR b = 2 AND c = 3")
	if err != nil {
		t.Fatalf("parseSQL92: %v", err)
	}
	or, ok := n.(*orNode)
	if !ok {
		t.Fatalf("顶层 = %T，期望 *orNode", n)
	}
	if _, ok := or.left.(*cmpNode); !ok {
		t.Fatalf("or.left = %T，期望 *cmpNode", or.left)
	}
	if _, ok := or.right.(*andNode); !ok {
		t.Fatalf("or.right = %T，期望 *andNode（AND 优先级高于 OR）", or.right)
	}
}

func TestParseSQL92BetweenSwallowsItsOwnAnd(t *testing.T) {
	// 本解析器最经典的错处：BETWEEN 的 AND 必须由 BETWEEN 分支自己消费，
	// 不能被 andExpr 层抢走。
	// a BETWEEN 1 AND 2 AND b = 3 必须是 and(between(a,1,2), cmp(b,3))，
	// 而不是 between(a, 1, and(2, ...)) 或解析失败。
	n, err := parseSQL92("a BETWEEN 1 AND 2 AND b = 3")
	if err != nil {
		t.Fatalf("parseSQL92: %v", err)
	}
	and, ok := n.(*andNode)
	if !ok {
		t.Fatalf("顶层 = %T，期望 *andNode", n)
	}
	bt, ok := and.left.(*betweenNode)
	if !ok {
		t.Fatalf("and.left = %T，期望 *betweenNode", and.left)
	}
	if bt.ident != "a" || bt.lo.num != 1 || bt.hi.num != 2 {
		t.Fatalf("between 解析错位: %+v", bt)
	}
	if _, ok := and.right.(*cmpNode); !ok {
		t.Fatalf("and.right = %T，期望 *cmpNode", and.right)
	}
}

func TestParseSQL92Paren(t *testing.T) {
	// 括号改变优先级：(a = 1 OR b = 2) AND c = 3 顶层必须是 and
	n, err := parseSQL92("(a = 1 OR b = 2) AND c = 3")
	if err != nil {
		t.Fatalf("parseSQL92: %v", err)
	}
	and, ok := n.(*andNode)
	if !ok {
		t.Fatalf("顶层 = %T，期望 *andNode", n)
	}
	if _, ok := and.left.(*orNode); !ok {
		t.Fatalf("and.left = %T，期望 *orNode", and.left)
	}
}

func TestParseSQL92Forms(t *testing.T) {
	// 各形态能解析出正确的节点类型与字段
	cases := []struct {
		expr  string
		check func(t *testing.T, n node)
	}{
		{"a IS NULL", func(t *testing.T, n node) {
			v := n.(*isNullNode)
			if v.ident != "a" || v.negated {
				t.Fatalf("%+v", v)
			}
		}},
		{"a IS NOT NULL", func(t *testing.T, n node) {
			v := n.(*isNullNode)
			if v.ident != "a" || !v.negated {
				t.Fatalf("%+v", v)
			}
		}},
		{"a NOT IN (1, 2, 3)", func(t *testing.T, n node) {
			v := n.(*inNode)
			if v.ident != "a" || !v.negated || len(v.vals) != 3 {
				t.Fatalf("%+v", v)
			}
		}},
		{"a NOT BETWEEN 1 AND 2", func(t *testing.T, n node) {
			v := n.(*betweenNode)
			if !v.negated {
				t.Fatalf("%+v", v)
			}
		}},
		{"10 < age", func(t *testing.T, n node) {
			// 常量在左：解析器把它归一化为 ident 在左的形式并翻转运算符，
			// 求值层因此只需处理一种朝向
			v := n.(*cmpNode)
			if v.ident != "age" || v.op != ">" {
				t.Fatalf("常量在左未归一化: %+v", v)
			}
		}},
		{"NOT a = 1", func(t *testing.T, n node) {
			if _, ok := n.(*notNode); !ok {
				t.Fatalf("%T", n)
			}
		}},
		{"k = TRUE", func(t *testing.T, n node) {
			v := n.(*cmpNode)
			if v.val.kind != constBool || !v.val.b {
				t.Fatalf("%+v", v)
			}
		}},
	}
	for _, c := range cases {
		n, err := parseSQL92(c.expr)
		if err != nil {
			t.Fatalf("parseSQL92(%q): %v", c.expr, err)
		}
		c.check(t, n)
	}
}

func TestParseSQL92IntConstantKeepsPrecision(t *testing.T) {
	// 整数常量必须同时保留 int64 形式：仅存 float64 会让大整数在
	// 解析阶段就丢精度，Task 5 的精确整数比较无从谈起。
	n, err := parseSQL92("id = 9007199254740993") // 2^53 + 1
	if err != nil {
		t.Fatalf("parseSQL92: %v", err)
	}
	v := n.(*cmpNode)
	if !v.val.isInt || v.val.i64 != 9007199254740993 {
		t.Fatalf("大整数常量丢精度: %+v", v.val)
	}
}

func TestParseSQL92SyntaxErrors(t *testing.T) {
	cases := []struct{ expr, wantIn string }{
		{"a = ", "期望"},
		{"a BETWEEN 1 2", "AND"},     // BETWEEN 缺 AND
		{"a IN ()", "至少"},           // IN 列表为空
		{"a IN (1, 2", "')'"},        // 括号未闭合
		{"(a = 1", "')'"},
		{"1 IS NULL", "属性名"},    // IS NULL 左侧必须是属性名
		{"'x' IN ('x')", "属性名"}, // IN 左侧必须是属性名
		{"a = b", "属性名"},         // 属性对属性，文法层排除
		{"1 = 1", "属性名"},         // 常量对常量，文法层排除
		{"a = 1 AND", "期望"},
	}
	for _, c := range cases {
		_, err := parseSQL92(c.expr)
		if err == nil {
			t.Fatalf("parseSQL92(%q) 期望报错，实际 nil", c.expr)
		}
		if !strings.Contains(err.Error(), c.wantIn) {
			t.Fatalf("parseSQL92(%q) 错误 %q 未包含 %q", c.expr, err.Error(), c.wantIn)
		}
		if !strings.Contains(err.Error(), "第") {
			t.Fatalf("parseSQL92(%q) 错误 %q 缺列号", c.expr, err.Error())
		}
	}
}

func TestBuildSQLFilterSemanticRejects(t *testing.T) {
	cases := []struct{ expr, wantIn string }{
		{"k > 'abc'", "字符串"},           // 字符串不支持大小比较
		{"k >= 'abc'", "字符串"},
		{"k = NULL", "IS NULL"},           // 必须提示改用 IS NULL
		{"1 = 1", "属性名"},                // 常量对常量
		{"a = b", "属性名"},                // 属性对属性
		{"k IN (1, 'a')", "类型"},          // IN 列表类型混用
		{"k BETWEEN 1 AND 'z'", "类型"},    // BETWEEN 上下界类型混用
	}
	for _, c := range cases {
		_, err := buildSQLFilter(c.expr)
		if err == nil {
			t.Fatalf("buildSQLFilter(%q) 期望报错，实际 nil", c.expr)
		}
		if !strings.Contains(err.Error(), c.wantIn) {
			t.Fatalf("buildSQLFilter(%q) 错误 %q 未包含 %q", c.expr, err.Error(), c.wantIn)
		}
	}
}

func TestBuildSQLFilterEqNullMessageIsActionable(t *testing.T) {
	// = NULL 的错误信息必须直接给出可照做的替代写法，不能只说"不支持"。
	// 这是相对 SQL 标准的有意偏离（标准里它合法且恒 UNKNOWN），
	// 偏离的价值全在这句提示上。
	_, err := buildSQLFilter("k = NULL")
	if err == nil {
		t.Fatal("期望报错")
	}
	if !strings.Contains(err.Error(), "k IS NULL") {
		t.Fatalf("错误 %q 未给出 `k IS NULL` 的替代写法", err.Error())
	}
}

func TestBuildSQLFilterLimits(t *testing.T) {
	// 长度上限
	long := "k = '" + strings.Repeat("x", maxExprBytes) + "'"
	if _, err := buildSQLFilter(long); err == nil || !strings.Contains(err.Error(), "长度") {
		t.Fatalf("超长表达式未被拒或错误不含'长度': %v", err)
	}
	// 深度上限：嵌套括号
	deep := strings.Repeat("(", maxASTDepth+2) + "a = 1" + strings.Repeat(")", maxASTDepth+2)
	if _, err := buildSQLFilter(deep); err == nil || !strings.Contains(err.Error(), "深度") {
		t.Fatalf("超深表达式未被拒或错误不含'深度': %v", err)
	}
	// IN 元素数上限
	elems := make([]string, maxInElems+1)
	for i := range elems {
		elems[i] = strconv.Itoa(i)
	}
	big := "k IN (" + strings.Join(elems, ",") + ")"
	if _, err := buildSQLFilter(big); err == nil || !strings.Contains(err.Error(), "IN") {
		t.Fatalf("超大 IN 列表未被拒或错误不含'IN': %v", err)
	}
}

func TestBuildSQLFilterAcceptsValid(t *testing.T) {
	// 正常表达式必须通过——上限用例容易写成"什么都拒"，需要反向对照
	for _, expr := range []string{
		"a = 1",
		"a > 1 AND b = 'x'",
		"a BETWEEN 1 AND 10 OR NOT c IS NULL",
		"k IN ('x', 'y', 'z')",
		"flag = TRUE",
		"10 < age",
	} {
		if _, err := buildSQLFilter(expr); err != nil {
			t.Fatalf("buildSQLFilter(%q) 意外报错: %v", expr, err)
		}
	}
}
