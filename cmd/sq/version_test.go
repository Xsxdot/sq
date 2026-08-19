package main

import (
	"strings"
	"testing"
)

// 默认值必须让本地裸 go build 出来的二进制也能跑通 --version，
// 不能是空串——空串会打印出三行只有标签没有值的输出，看起来像坏了。
func TestVersionStringUsesDefaultsWhenNotInjected(t *testing.T) {
	got := versionString()
	for _, want := range []string{"sq dev", "commit: none", "built: unknown"} {
		if !strings.Contains(got, want) {
			t.Errorf("versionString() 缺少 %q，实际输出：\n%s", want, got)
		}
	}
}

// ldflags 注入后三个值都要如实出现。这里直接改包级变量模拟注入，
// 因为 -ldflags 的效果无法在单测里构造。
func TestVersionStringReflectsInjectedValues(t *testing.T) {
	oldV, oldC, oldD := version, commit, buildDate
	t.Cleanup(func() { version, commit, buildDate = oldV, oldC, oldD })

	version, commit, buildDate = "0.1.0", "abc1234", "2026-08-19T00:00:00Z"
	got := versionString()
	for _, want := range []string{"sq 0.1.0", "commit: abc1234", "built: 2026-08-19T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Errorf("versionString() 缺少 %q，实际输出：\n%s", want, got)
		}
	}
}

// 输出必须以换行结尾——它直接喂给 fmt.Print，缺换行会和 shell 提示符黏在一起。
func TestVersionStringEndsWithNewline(t *testing.T) {
	if got := versionString(); !strings.HasSuffix(got, "\n") {
		t.Errorf("versionString() 未以换行结尾：%q", got)
	}
}
