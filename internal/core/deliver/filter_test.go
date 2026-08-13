// ParseFilter 分派与三值匹配单测。
package deliver

import (
	"testing"

	"github.com/xushixin/sq/internal/core"
)

func TestParseFilterAllPass(t *testing.T) {
	// "*"、空串、纯空白都必须拿到 AllPass，而不是一个"恰好放行一切"的
	// TagFilter。两者行为相同但含义不同：AllPass 表示"不过滤"，
	// 空集 TagFilter 表示"过滤且什么都不匹配"，后者会把消息全丢掉。
	//
	// 注：allPass 是空结构体，该比较判定的是动态类型而非指针身份——
	// 这正是需要的，够挡住"返回了 TagFilter"这个真实错法。
	for _, expr := range []string{"*", "", "   "} {
		f, err := ParseFilter(FilterTag, expr)
		if err != nil {
			t.Fatalf("ParseFilter(FilterTag, %q) 报错: %v", expr, err)
		}
		if f != AllPass {
			t.Fatalf("ParseFilter(FilterTag, %q) = %#v，期望 AllPass 单例", expr, f)
		}
	}
	if got := AllPass.Match(&core.Message{Tag: "anything"}); got != ResultTrue {
		t.Fatalf("AllPass.Match = %v，期望 ResultTrue", got)
	}
}

func TestTagFilterMatchTriState(t *testing.T) {
	f, err := ParseFilter(FilterTag, "a || b")
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}
	cases := []struct {
		tag  string
		want Result
	}{
		{"a", ResultTrue},
		{"b", ResultTrue},
		{"c", ResultFalse},
		{"", ResultFalse},
	}
	for _, c := range cases {
		// TAG 过滤永不产生 ResultUnknown：tag 是消息的固有字段，
		// 没有"无法判定"的情形。这条断言守住这个不变式。
		if got := f.Match(&core.Message{Tag: c.tag}); got != c.want {
			t.Fatalf("tag %q: Match = %v，期望 %v", c.tag, got, c.want)
		}
	}
}

func TestParseFilterTagRejectsEmptyTokens(t *testing.T) {
	// 保留旧 TestParseTagFilter 对「空 tag token」的拒绝覆盖：
	// "||" 两侧都必须有非空白内容，"a ||"、"||"、"a || || b" 均非法。
	for _, expr := range []string{"a ||", "||", "a || || b"} {
		if _, err := ParseFilter(FilterTag, expr); err == nil {
			t.Fatalf("ParseFilter(FilterTag, %q) 应拒绝空 tag token，实际 nil", expr)
		}
	}
}

func TestParseFilterSQL92Wired(t *testing.T) {
	// SQL92 分支已接线：合法表达式必须产出可用的 SQLFilter，而不是报错
	// 或静默当成 AllPass（静默放行会让所有 SQL92 订阅收到全量消息）。
	// 非法表达式仍须报错，把错误回给客户端（ILLEGAL_FILTER_EXPRESSION）。
	flt, err := ParseFilter(FilterSQL92, "age > 10 AND TAGS = 'a'")
	if err != nil {
		t.Fatalf("ParseFilter(FilterSQL92, 合法表达式) 报错: %v", err)
	}
	if _, ok := flt.(*SQLFilter); !ok {
		t.Fatalf("ParseFilter(FilterSQL92) = %#v，期望 *SQLFilter", flt)
	}
	if _, err := ParseFilter(FilterSQL92, "k = NULL"); err == nil {
		t.Fatal("ParseFilter(FilterSQL92, 非法表达式) 应报错，实际 nil")
	}
}

func TestParseFilterUnknownKind(t *testing.T) {
	if _, err := ParseFilter(FilterKind(99), "*"); err == nil {
		t.Fatal("未知 FilterKind 期望报错，实际 nil")
	}
}
