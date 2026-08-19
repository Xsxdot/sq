// sql92_eval_test.go：SQL92 三值求值行为契约测试。
//
// 职责：
//   - 锁定三值真值表（AND/OR/NOT 全格）、类型强转、TAGS 映射与
//     BETWEEN/IN 与展开式逐格等价
//
// 边界：
//   - 只测求值层，不测语法（那是 sql92_parse_test 的事）
package deliver

import (
	"testing"

	"github.com/Xsxdot/sq/internal/core"
)

// mustFilter 构建过滤器，表达式非法直接 Fatal——求值用例里的表达式
// 都应是合法的，非法说明用例本身写错了。
func mustFilter(t *testing.T, expr string) *SQLFilter {
	t.Helper()
	f, err := buildSQLFilter(expr)
	if err != nil {
		t.Fatalf("buildSQLFilter(%q): %v", expr, err)
	}
	return f
}

func msgWith(props map[string]string) *core.Message {
	return &core.Message{Properties: props}
}

func TestEvalTruthTableAnd(t *testing.T) {
	// AND 全 9 格。用 a/b 两个属性造出 T/F/U 三种子结果：
	//   T: x = 1 且 x="1"      F: x = 1 且 x="2"      U: x = 1 且 x 缺失
	val := map[Result]map[string]string{
		ResultTrue:    {"a": "1"},
		ResultFalse:   {"a": "2"},
		ResultUnknown: {},
	}
	valB := map[Result]map[string]string{
		ResultTrue:    {"b": "1"},
		ResultFalse:   {"b": "2"},
		ResultUnknown: {},
	}
	want := map[Result]map[Result]Result{
		ResultTrue:    {ResultTrue: ResultTrue, ResultFalse: ResultFalse, ResultUnknown: ResultUnknown},
		ResultFalse:   {ResultTrue: ResultFalse, ResultFalse: ResultFalse, ResultUnknown: ResultFalse},
		ResultUnknown: {ResultTrue: ResultUnknown, ResultFalse: ResultFalse, ResultUnknown: ResultUnknown},
	}
	f := mustFilter(t, "a = 1 AND b = 1")
	for l, lp := range val {
		for r, rp := range valB {
			props := map[string]string{}
			for k, v := range lp {
				props[k] = v
			}
			for k, v := range rp {
				props[k] = v
			}
			if got := f.Match(msgWith(props)); got != want[l][r] {
				t.Fatalf("%v AND %v = %v，期望 %v（props=%v）", l, r, got, want[l][r], props)
			}
		}
	}
}

func TestEvalTruthTableOr(t *testing.T) {
	// OR 全 9 格，构造方式同 AND
	val := map[Result]map[string]string{
		ResultTrue:    {"a": "1"},
		ResultFalse:   {"a": "2"},
		ResultUnknown: {},
	}
	valB := map[Result]map[string]string{
		ResultTrue:    {"b": "1"},
		ResultFalse:   {"b": "2"},
		ResultUnknown: {},
	}
	want := map[Result]map[Result]Result{
		ResultTrue:    {ResultTrue: ResultTrue, ResultFalse: ResultTrue, ResultUnknown: ResultTrue},
		ResultFalse:   {ResultTrue: ResultTrue, ResultFalse: ResultFalse, ResultUnknown: ResultUnknown},
		ResultUnknown: {ResultTrue: ResultTrue, ResultFalse: ResultUnknown, ResultUnknown: ResultUnknown},
	}
	f := mustFilter(t, "a = 1 OR b = 1")
	for l, lp := range val {
		for r, rp := range valB {
			props := map[string]string{}
			for k, v := range lp {
				props[k] = v
			}
			for k, v := range rp {
				props[k] = v
			}
			if got := f.Match(msgWith(props)); got != want[l][r] {
				t.Fatalf("%v OR %v = %v，期望 %v（props=%v）", l, r, got, want[l][r], props)
			}
		}
	}
}

func TestEvalNotUnknownStaysUnknown(t *testing.T) {
	// 三值逻辑最容易退化成二值的一格，单列一条守住：
	// 属性缺失时 NOT (a > 10) 必须是 UNKNOWN（不投递），
	// 而不是 TRUE（把用户明确不想要的消息投出去）。
	f := mustFilter(t, "NOT a > 10")
	if got := f.Match(msgWith(map[string]string{})); got != ResultUnknown {
		t.Fatalf("NOT UNKNOWN = %v，期望 ResultUnknown", got)
	}
	if got := f.Match(msgWith(map[string]string{"a": "5"})); got != ResultTrue {
		t.Fatalf("NOT FALSE = %v，期望 ResultTrue", got)
	}
	if got := f.Match(msgWith(map[string]string{"a": "50"})); got != ResultFalse {
		t.Fatalf("NOT TRUE = %v，期望 ResultFalse", got)
	}
}

func TestEvalTypeCoercion(t *testing.T) {
	cases := []struct {
		expr  string
		props map[string]string
		want  Result
	}{
		// 数值档
		{"a > 10", map[string]string{"a": "11"}, ResultTrue},
		{"a > 10", map[string]string{"a": "9"}, ResultFalse},
		{"a > 10", map[string]string{"a": "abc"}, ResultUnknown}, // 非数字撞数值比较
		{"a > 10", map[string]string{}, ResultUnknown},           // 属性缺失
		{"a > 10.5", map[string]string{"a": "10.6"}, ResultTrue}, // 小数
		// 字符串档
		{"s = 'x'", map[string]string{"s": "x"}, ResultTrue},
		{"s = 'x'", map[string]string{"s": "y"}, ResultFalse},
		{"s <> 'x'", map[string]string{"s": "y"}, ResultTrue},
		{"s = 'x'", map[string]string{}, ResultUnknown},
		// 布尔档：属性值大小写不敏感
		{"f = TRUE", map[string]string{"f": "true"}, ResultTrue},
		{"f = TRUE", map[string]string{"f": "TRUE"}, ResultTrue},
		{"f = TRUE", map[string]string{"f": "True"}, ResultTrue},
		{"f = TRUE", map[string]string{"f": "false"}, ResultFalse},
		{"f = TRUE", map[string]string{"f": "yes"}, ResultUnknown}, // 非布尔字面量
		// 数值比较不受字符串形态影响：'007' 与 7 在数值档下相等
		{"a = 7", map[string]string{"a": "007"}, ResultTrue},
	}
	for _, c := range cases {
		f := mustFilter(t, c.expr)
		if got := f.Match(msgWith(c.props)); got != c.want {
			t.Fatalf("%q with %v = %v，期望 %v", c.expr, c.props, got, c.want)
		}
	}
}

func TestEvalInt64PrecisionBoundary(t *testing.T) {
	// 看门用例：两个相差 1 且都大于 2^53 的雪花 ID 必须判不等。
	// 退回纯 float64 实现时这条必红——float64 在 2^53 以上无法表示
	// 相邻整数，两个不同的 ID 会被判为相等（静默的错误匹配）。
	f := mustFilter(t, "id = 9007199254740993") // 2^53 + 1
	if got := f.Match(msgWith(map[string]string{"id": "9007199254740992"})); got != ResultFalse {
		t.Fatalf("2^53 与 2^53+1 判定 = %v，期望 ResultFalse（float64 精度陷阱）", got)
	}
	if got := f.Match(msgWith(map[string]string{"id": "9007199254740993"})); got != ResultTrue {
		t.Fatalf("同值判定 = %v，期望 ResultTrue", got)
	}
}

func TestEvalTagsMapping(t *testing.T) {
	// TAGS 映射到 m.Tag（tag 是独立字段，不在 Properties 里）
	f := mustFilter(t, "TAGS = 'order'")
	if got := f.Match(&core.Message{Tag: "order"}); got != ResultTrue {
		t.Fatalf("TAGS 未映射到 m.Tag: %v", got)
	}
	if got := f.Match(&core.Message{Tag: "pay"}); got != ResultFalse {
		t.Fatalf("TAGS = %v，期望 ResultFalse", got)
	}
	// 同名用户属性被系统映射遮蔽
	shadowed := &core.Message{Tag: "order", Properties: map[string]string{"TAGS": "hijack"}}
	if got := f.Match(shadowed); got != ResultTrue {
		t.Fatalf("同名用户属性未被遮蔽: %v", got)
	}
	// tag 为空时 TAGS 视为缺失，而不是空串
	if got := mustFilter(t, "TAGS IS NULL").Match(&core.Message{Tag: ""}); got != ResultTrue {
		t.Fatalf("空 tag 应视为 TAGS 缺失: %v", got)
	}
}

func TestEvalIsNull(t *testing.T) {
	cases := []struct {
		expr  string
		props map[string]string
		want  Result
	}{
		{"a IS NULL", map[string]string{}, ResultTrue},
		{"a IS NULL", map[string]string{"a": "1"}, ResultFalse},
		{"a IS NOT NULL", map[string]string{}, ResultFalse},
		{"a IS NOT NULL", map[string]string{"a": "1"}, ResultTrue},
		// IS NULL 永不返回 UNKNOWN，即便属性值是无法解释的字符串
		{"a IS NOT NULL", map[string]string{"a": "abc"}, ResultTrue},
	}
	for _, c := range cases {
		f := mustFilter(t, c.expr)
		if got := f.Match(msgWith(c.props)); got != c.want {
			t.Fatalf("%q with %v = %v，期望 %v", c.expr, c.props, got, c.want)
		}
	}
}

func TestEvalBetweenAndInEqualToExpansion(t *testing.T) {
	// BETWEEN / IN 必须与其展开形式逐格等价（含三值传播）
	props := []map[string]string{
		{"a": "5"}, {"a": "10"}, {"a": "15"}, {"a": "20"}, {"a": "25"}, {"a": "x"}, {},
	}
	bt := mustFilter(t, "a BETWEEN 10 AND 20")
	expanded := mustFilter(t, "a >= 10 AND a <= 20")
	for _, p := range props {
		if g1, g2 := bt.Match(msgWith(p)), expanded.Match(msgWith(p)); g1 != g2 {
			t.Fatalf("BETWEEN 与展开式不等价 props=%v: %v vs %v", p, g1, g2)
		}
	}
	in := mustFilter(t, "a IN (5, 15)")
	inExpanded := mustFilter(t, "a = 5 OR a = 15")
	for _, p := range props {
		if g1, g2 := in.Match(msgWith(p)), inExpanded.Match(msgWith(p)); g1 != g2 {
			t.Fatalf("IN 与展开式不等价 props=%v: %v vs %v", p, g1, g2)
		}
	}
	// NOT BETWEEN / NOT IN 同样等价于对展开式取 NOT（含 NOT UNKNOWN = UNKNOWN）
	nbt := mustFilter(t, "a NOT BETWEEN 10 AND 20")
	nExpanded := mustFilter(t, "NOT (a >= 10 AND a <= 20)")
	for _, p := range props {
		if g1, g2 := nbt.Match(msgWith(p)), nExpanded.Match(msgWith(p)); g1 != g2 {
			t.Fatalf("NOT BETWEEN 与展开式不等价 props=%v: %v vs %v", p, g1, g2)
		}
	}
}
