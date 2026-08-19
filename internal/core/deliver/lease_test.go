// Package deliver 实现 POP 消费：投递、确认、不可见超时重投、长轮询。
//
// 职责（lease 判据测试文件）：
//   - 验证 Lease.Enabled() 的三字段齐全判据
//   - 验证 renewable() 的四条判据（启用续租 / 预算未耗尽 / 归属已知 / 持有者存活）
//   - 验证存活判定问的是记录里的持有者而非本次轮询方（重平衡场景）
//
// 边界：
//   - 不测续租与投递路径的交互——那部分在 deliver_test.go 的
//     TestReceiveRenews* 系列里验证
//   - 不测 rpc 层会话存活判定的真实实现——deliver 只收闭包，闭包来自 rpc
package deliver

import (
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/core"
)

func TestLeaseEnabled(t *testing.T) {
	alive := func(string) bool { return true }
	cases := []struct {
		name string
		l    Lease
		want bool
	}{
		{"三项齐全", Lease{Owner: "c", MaxRenew: time.Minute, Alive: alive}, true},
		{"缺 Owner", Lease{MaxRenew: time.Minute, Alive: alive}, false},
		{"MaxRenew 为零", Lease{Owner: "c", Alive: alive}, false},
		{"MaxRenew 为负", Lease{Owner: "c", MaxRenew: -time.Second, Alive: alive}, false},
		{"缺 Alive", Lease{Owner: "c", MaxRenew: time.Minute}, false},
		{"全空", Lease{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.l.Enabled(); got != tc.want {
				t.Fatalf("Enabled() = %v，期望 %v", got, tc.want)
			}
		})
	}
}

func TestRenewableCriteria(t *testing.T) {
	const now = int64(1_000_000)
	okLease := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(string) bool { return true }}
	deadLease := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(string) bool { return false }}
	fresh := func() *core.InflightState {
		return &core.InflightState{ExpireAtMs: now - 1, Attempts: 1, Owner: "holder", RenewUntilMs: now + 1000}
	}

	if !renewable(okLease, fresh(), now) {
		t.Fatal("四条判据全真时应当续租")
	}
	// 判据 1：本次取件未启用续租（如 SimpleConsumer 接管了这个队列）
	if renewable(Lease{}, fresh(), now) {
		t.Fatal("判据 1：未启用续租的取件不应续租")
	}
	// 判据 2：旧格式记录（RenewUntilMs 为 0）
	st := fresh()
	st.RenewUntilMs = 0
	if renewable(okLease, st, now) {
		t.Fatal("判据 2：旧格式记录不应续租")
	}
	// 判据 2：续租预算已耗尽（边界值——等于 now 即算耗尽）
	st = fresh()
	st.RenewUntilMs = now
	if renewable(okLease, st, now) {
		t.Fatal("判据 2：预算恰好耗尽时不应续租")
	}
	// 判据 3：归属未知
	st = fresh()
	st.Owner = ""
	if renewable(okLease, st, now) {
		t.Fatal("判据 3：归属未知时不应续租")
	}
	// 判据 4：持有者已断线
	if renewable(deadLease, fresh(), now) {
		t.Fatal("判据 4：持有者已断线时不应续租")
	}
}

func TestRenewableChecksRecordOwnerNotPoller(t *testing.T) {
	// 判据 4 判的是记录里的持有者，不是本次轮询方——重平衡后两者会不同
	const now = int64(1_000_000)
	var asked string
	l := Lease{Owner: "poller", MaxRenew: time.Minute, Alive: func(id string) bool {
		asked = id
		return true
	}}
	st := &core.InflightState{ExpireAtMs: now - 1, Owner: "holder", RenewUntilMs: now + 1000}
	renewable(l, st, now)
	if asked != "holder" {
		t.Fatalf("存活判定问的是 %q，应当问记录里的持有者 \"holder\"", asked)
	}
}
