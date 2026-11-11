// SQ4.9 H2：InflightCount 只在内存态仍权威时给 ok=true——本节点失去该组
// 领导权、或可写门 epoch 已不是内存态装载时的 epoch（内存态尚未被丢弃），
// 一律 ok=false，调用方回落读 pebble；否则前 leader 会一直报冻结的旧值。
package deliver

import (
	"context"
	"testing"
	"time"
)

func TestInflightCountNotAuthoritativeAfterLeadershipLoss(t *testing.T) {
	f := newHoldFixture(t)
	f.send(t, 3)
	if _, err := f.dl.Receive(context.Background(), "g", "t", 0, 2, time.Minute, 0, AllPass); err != nil {
		t.Fatal(err)
	}
	if n, ok := f.dl.InflightCount("g", "t", 0); !ok || n != 2 {
		t.Fatalf("leader 上应 ok=true n=2，得 n=%d ok=%v", n, ok)
	}
	f.rt.down.Store(true)
	if n, ok := f.dl.InflightCount("g", "t", 0); ok {
		t.Fatalf("失去领导权后应 ok=false，得 n=%d ok=true", n)
	}
	f.rt.down.Store(false)
	f.rt.epoch.Store(2)
	if n, ok := f.dl.InflightCount("g", "t", 0); ok {
		t.Fatalf("epoch 已变（内存态仍是旧 epoch）应 ok=false，得 n=%d ok=true", n)
	}
	// 新 epoch 下再访问即按盘重装，读数恢复权威
	if _, err := f.dl.Receive(context.Background(), "g", "t", 0, 1, time.Minute, 0, AllPass); err != nil {
		t.Fatal(err)
	}
	if n, ok := f.dl.InflightCount("g", "t", 0); !ok || n != 3 {
		t.Fatalf("重装后应 ok=true n=3，得 n=%d ok=%v", n, ok)
	}
}
