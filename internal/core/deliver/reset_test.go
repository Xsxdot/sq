// ResetCursor（Admin API 位点重置）测试。独立成文件：deliver_test.go 是 M4
// 并行改动区，本文件只新增不修改，避免合并冲突。
package deliver

import (
	"context"
	"testing"
	"time"
)

// TestResetCursorRewindsAndClearsInflight 重置到 0 后：inflight 清空、
// 消息从头重新投递、投递次数从 1 重新计（旧 inflight 已删，不是重投）。
func TestResetCursorRewindsAndClearsInflight(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t-reset", "m-0")
	f.send(t, "t-reset", "m-1")
	got, err := f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("首轮应收 2 条: %d %v", len(got), err)
	}
	// 未 ack 期间正常路径收不到任何东西（都在 inflight 里且未过期）
	if got, _ := f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil); len(got) != 0 {
		t.Fatalf("未过期不应重投，得到 %d 条", len(got))
	}
	if err := f.dl.ResetCursor(context.Background(), "g", "t-reset", 0, 0); err != nil {
		t.Fatal(err)
	}
	got, err = f.dl.Receive(context.Background(), "g", "t-reset", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("重置后应从头重新收 2 条: %d %v", len(got), err)
	}
	if string(got[0].Body) != "m-0" || got[0].DeliveryAttempt != 1 {
		t.Fatalf("重置后首条应为 m-0 且 attempt=1（inflight 已清空，属首投而非重投）: body=%s attempt=%d",
			got[0].Body, got[0].DeliveryAttempt)
	}
}

// TestResetCursorForwardSkips 向前重置 = 跳过消息（运维快进场景）。
func TestResetCursorForwardSkips(t *testing.T) {
	f := newFixture(t)
	f.send(t, "t-skip", "m-0")
	f.send(t, "t-skip", "m-1")
	if err := f.dl.ResetCursor(context.Background(), "g", "t-skip", 0, 1); err != nil {
		t.Fatal(err)
	}
	got, err := f.dl.Receive(context.Background(), "g", "t-skip", 0, 2, time.Minute, 0, nil)
	if err != nil || len(got) != 1 || string(got[0].Body) != "m-1" {
		t.Fatalf("快进后应只收 m-1: %v %v", got, err)
	}
}
