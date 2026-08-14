package store

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestPebbleLoggerLevelMapping 验证三件事：Infof 降级到 DEBUG、Errorf 对应
// ERROR、格式化参数被正确展开，且都带上 src=pebble 标记。
func TestPebbleLoggerLevelMapping(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newPebbleLogger(lg)

	p.Infof("wal count n=%d", 42)
	p.Errorf("compaction failed: %s", "boom")

	out := buf.String()
	for _, want := range []string{"level=DEBUG", "n=42", "level=ERROR", "compaction failed: boom", "src=pebble"} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出中缺少 %q，实际输出：%s", want, out)
		}
	}
}

// TestPebbleLoggerInfoFilteredAtInfoLevel 是本次改动的核心收益断言：默认的
// info 级别下，Pebble 的 Infof 噪声被完全挡住，不再污染日志。
func TestPebbleLoggerInfoFilteredAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	newPebbleLogger(lg).Infof("noisy: %s", "Found 0 WALs")
	if buf.Len() != 0 {
		t.Fatalf("info 级别下 Pebble 的 Infof 不应输出，实际：%s", buf.String())
	}
}

// TestOpenWiresPebbleLogger 证明适配器真的挂到了 pebble.Options.Logger 上，
// 而不是写了个没人用的类型。Pebble 在 open.go 的恢复路径上无条件打一行
// "Found %d WALs"（v2.1.6 open.go:383），所以一次空库 Open 就足以触发。
func TestOpenWiresPebbleLogger(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	st, err := Open(t.TempDir(), false, lg)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer st.Close()

	out := buf.String()
	if !strings.Contains(out, "src=pebble") {
		t.Fatalf("Open 未把适配器接到 pebble.Options.Logger，输出中没有 src=pebble 的行：%s", out)
	}
	if !strings.Contains(out, "WALs") {
		t.Fatalf("未捕获到 Pebble open 路径的 \"Found N WALs\" 日志：%s", out)
	}
}
