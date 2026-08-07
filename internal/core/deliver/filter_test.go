// TagFilter 解析与匹配单测。
package deliver

import "testing"

func TestParseTagFilter(t *testing.T) {
	if f, err := ParseTagFilter("*"); f != nil || err != nil {
		t.Fatalf("* 应解析为 nil 过滤器: %v %v", f, err)
	}
	if f, err := ParseTagFilter(""); f != nil || err != nil {
		t.Fatalf("空串应解析为 nil 过滤器: %v %v", f, err)
	}
	f, err := ParseTagFilter("a || b")
	if err != nil {
		t.Fatalf("多 tag 解析: %v", err)
	}
	for tag, want := range map[string]bool{"a": true, "b": true, "c": false, "": false} {
		if f.Match(tag) != want {
			t.Fatalf("Match(%q) 应为 %v", tag, want)
		}
	}
	if !(*TagFilter)(nil).Match("anything") {
		t.Fatal("nil 过滤器应匹配一切")
	}
	for _, bad := range []string{"a ||", "||", "a || || b"} {
		if _, err := ParseTagFilter(bad); err == nil {
			t.Fatalf("应拒绝非法表达式 %q", bad)
		}
	}
}
