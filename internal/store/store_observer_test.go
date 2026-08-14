// Apply 观测钩子的并发安全判别器。
//
// 职责：证明「一边跑 Apply 一边换观测钩子」不构成数据竞态。
// 边界：不验证观测值的准确性（切换瞬间漏掉少数样本是允许的，见 SetApplyObserver
//       的文档），只验证并发存取本身安全。
package store

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestApplyObserverConcurrentSetAndRead【承重】写侧与读侧并发。
//
// 这条用例存在的唯一理由：2026-08-14 实测发现集群档下 cluster.Manager.Start
// 拉起的 apply goroutine 会在 metrics 装配之前就开始读这个钩子（backlog B14）。
// 把实现退回裸 `var OnApplyObserve func(time.Duration)`，本用例在 -race 下必须变红；
// 若仍绿，说明用例没有真正制造并发读写，停下来上报，不要改断言迁就。
//
// 为什么写侧不「装/清交替」、而是每轮换一个新非 nil 闭包、循环结束后统一清一次：
// 第一版写侧是每轮 SetApplyObserver(fn) 紧接 SetApplyObserver(nil)，本机实测不稳定——
// 整包 `go test -race ./internal/store/` 11/11 红、隔离单跑 20/20 绿。根因是非 nil
// 窗口只存在于相邻两条语句之间，是纳秒级间隙，而读侧每个 goroutine 每次 Pebble
// 提交（微秒级）完成后才读一次钩子；本机负载高时，主 goroutine 打完整轮 toggle
// 循环期间读侧赶不上任何一次提交后读取，观测计数落 0——正确实现也会红。改成
// 「全程不切 nil」后并发暴露面不变：race detector 看的是「无同步地读写同一变量」，
// 与值是不是 nil 无关——但观测计数必然 >0，用例确定性绿。若有人想改回「来回切
// 更全面」，那正是本缺陷的复发点：改回去前先在本机跑一遍整包 -race 看红不红。
func TestApplyObserverConcurrentSetAndRead(t *testing.T) {
	s, err := Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	t.Cleanup(func() { SetApplyObserver(nil) })

	const writers = 4
	var observed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 读侧：多个 goroutine 持续 Apply，每次成功提交都会读一次钩子。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				b := s.NewBatch()
				if err := b.Set([]byte(fmt.Sprintf("k/%d/%d", w, i)), []byte("v")); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if err := s.Apply(b); err != nil {
					t.Errorf("Apply: %v", err)
					return
				}
			}
		}(w)
	}

	// 写侧：每轮换一个新的非 nil 闭包，全程不切 nil；循环结束后统一清一次。
	// 为什么这样（而不是「装/清」交替）：见函数头注释——非 nil 窗口若只存在于
	// 相邻两条语句之间就是纳秒级，读侧要等一次微秒级 Pebble 提交才读钩子，
	// 本机整包 -race 实测 11/11 红；全程保持非 nil 则并发暴露面不变（race
	// detector 看的是无同步读写同一变量，与值是否为 nil 无关）而观测必然 >0。
	deadline := time.Now().Add(10 * time.Second)
	for rounds := 0; ; rounds++ {
		SetApplyObserver(func(time.Duration) { observed.Add(1) })
		if rounds >= 200 && observed.Load() > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
	}
	SetApplyObserver(nil)
	close(stop)
	wg.Wait()

	// 不断言具体条数：切换瞬间漏样本是允许的。但一条都没观测到，说明
	// 钩子从未真正被调用过，用例失去判别力（10 秒预算已兜底，等不到就是
	// 读侧一条都没跑成，而不是没给够时间）。
	if observed.Load() == 0 {
		t.Fatal("观测计数为 0——10 秒预算内钩子一次都没被调用，本用例无法证明任何并发性质")
	}
}
