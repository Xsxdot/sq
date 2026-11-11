// sync_test.go 验证 Log.Sync 锁外落盘的并发语义（SQ4.4）。
//
// 职责：S1 刷盘进行中追加不被阻塞；I1 被轮转抢先关闭返回 nil；I2 被
// Close 抢先返回「已关闭」错误；I3 Close 后 Sync 仍报错；S2 轮转与刷盘
// 并发压测无错误、重开逐条校验无丢失。
//
// 边界：用未导出的 syncHook 把 Sync 停在「已取句柄、未落盘」这一点，
// 制造确定性的并发交错；不依赖计时统计。
package seglog

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// hookWait 测试等待上限：正常交错在毫秒级完成，2s 只在被阻塞时触发。
const hookWait = 2 * time.Second

// blockSync 给 l 装上阻塞钩子：Sync 进入钩子时关闭 entered，然后等待
// release 被调用。release 幂等，并注册到 t.Cleanup，失败路径也不会泄漏
// 卡死的 goroutine。必须在启动任何并发 Sync 之前调用。
func blockSync(t *testing.T, l *Log) (entered <-chan struct{}, release func()) {
	t.Helper()
	en := make(chan struct{})
	rel := make(chan struct{})
	var enOnce, relOnce sync.Once
	l.syncHook = func() {
		enOnce.Do(func() { close(en) })
		<-rel
	}
	release = func() { relOnce.Do(func() { close(rel) }) }
	t.Cleanup(release)
	return en, release
}

// startSync 在新 goroutine 里调 l.Sync，并等它进入钩子。
func startSync(t *testing.T, l *Log, entered <-chan struct{}) <-chan error {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- l.Sync() }()
	select {
	case <-entered:
	case <-time.After(hookWait):
		t.Fatal("Sync 未在锁外进入落盘前钩子（仍是全程持锁实现？）")
	}
	return errc
}

// waitErr 等 Sync 返回。
func waitErr(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(hookWait):
		t.Fatal("Sync 放行后未返回")
		return nil
	}
}

// openTestLog 打开日志并以 t.Cleanup 注册 Close（Close 幂等）。不用 defer：
// 失败路径上 defer 先于 Cleanup 执行，若此时 Sync 仍被钩子挡在锁内（变异
// 场景），defer 的 Close 会抢锁死等；Cleanup 按 LIFO 执行，blockSync 后
// 注册的 release 一定先于这里的 Close，失败即快速报红而不是挂到超时。
func openTestLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, _, _, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// TestSyncDoesNotBlockAppend S1：Sync 停在落盘前时，同一日志的 Append
// （含 HS）必须照常返回。
func TestSyncDoesNotBlockAppend(t *testing.T) {
	l := openTestLog(t, t.TempDir())
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a")}, false); err != nil {
		t.Fatal(err)
	}

	entered, release := blockSync(t, l)
	errc := startSync(t, l, entered)

	done := make(chan error, 1)
	go func() {
		for i := uint64(2); i <= 11; i++ {
			if err := l.Append(hs(1, 1, i-1), []*raftpb.Entry{ent(i, 1, "x")}, false); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("刷盘进行中 Append 失败: %v", err)
		}
	case <-time.After(hookWait):
		t.Fatal("刷盘进行中 Append 被阻塞（Sync 仍持日志锁落盘）")
	}

	release()
	if err := waitErr(t, errc); err != nil {
		t.Fatalf("Sync = %v; want nil", err)
	}
}

// TestSyncPreemptedByRotationReturnsNil I1：Sync 取到旧段句柄后被轮转
// 关闭——轮转屏障已对旧段完整 fsync，本次调用覆盖的写入已落盘，返回 nil。
func TestSyncPreemptedByRotationReturnsNil(t *testing.T) {
	old := SegMaxBytes
	SegMaxBytes = 256
	defer func() { SegMaxBytes = old }()

	dir := t.TempDir()
	l := openTestLog(t, dir)
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a")}, false); err != nil {
		t.Fatal(err)
	}

	entered, release := blockSync(t, l)
	errc := startSync(t, l, entered)

	// 一条超过段长的条目：本轮 Append 尾部必然轮转，关闭 Sync 手里的旧段句柄
	if err := l.Append(nil, []*raftpb.Entry{ent(2, 1, strings.Repeat("r", 300))}, false); err != nil {
		t.Fatal(err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) < 2 {
		t.Fatalf("段数 = %d; want ≥2（应已轮转）", len(segs))
	}

	release()
	if err := waitErr(t, errc); err != nil {
		t.Fatalf("被轮转抢先的 Sync = %v; want nil（轮转屏障已覆盖）", err)
	}
}

// TestSyncPreemptedByCloseReturnsClosedError I2：Sync 取到句柄后被 Close
// 关闭——Close 不刷盘，不能冒称已覆盖，必须报「已关闭」。
func TestSyncPreemptedByCloseReturnsClosedError(t *testing.T) {
	l := openTestLog(t, t.TempDir())
	if err := l.Append(hs(1, 1, 0), []*raftpb.Entry{ent(1, 1, "a")}, false); err != nil {
		t.Fatal(err)
	}

	entered, release := blockSync(t, l)
	errc := startSync(t, l, entered)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	release()
	err := waitErr(t, errc)
	if err == nil {
		t.Fatal("被 Close 抢先的 Sync 返回 nil; want 「已关闭」错误")
	}
	if !strings.Contains(err.Error(), "已关闭") {
		t.Fatalf("Sync 错误 = %v; want 含「已关闭」", err)
	}
}

// TestSyncAfterCloseFails I3：Close 后 Sync 仍报错（现有行为不回退）。
func TestSyncAfterCloseFails(t *testing.T) {
	l := openTestLog(t, t.TempDir())
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err == nil {
		t.Fatal("Close 后 Sync 返回 nil; want 错误")
	}
}

// TestRotateConcurrentWithSyncNoLoss S2：小段长强制高频轮转，刷盘循环与
// 追加并行；Sync 全程无错误，关闭后重开逐条校验 index/Data/HS。
func TestRotateConcurrentWithSyncNoLoss(t *testing.T) {
	old := SegMaxBytes
	SegMaxBytes = 256
	defer func() { SegMaxBytes = old }()

	const total = 5000
	dir := t.TempDir()
	rec := &recordHandler{}
	l, _, _, err := Open(dir, slog.New(rec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	// 拉宽「已取句柄、未落盘」窗口：不拉宽时锁外窗口只有几条指令，并发几乎
	// 撞不上「句柄被轮转关闭」这条分支，压测就测不到它。钩子等到下一次轮转
	// 发生（或 20ms 上限）再放行落盘，让多数 Sync 都与轮转撞车。
	l.syncHook = func() {
		l.mu.Lock()
		seq := l.activeSeq
		l.mu.Unlock()
		deadline := time.Now().Add(20 * time.Millisecond)
		for time.Now().Before(deadline) {
			l.mu.Lock()
			rotated := l.activeSeq != seq
			l.mu.Unlock()
			if rotated {
				return
			}
			time.Sleep(100 * time.Microsecond)
		}
	}

	data := func(i uint64) string { return fmt.Sprintf("payload-%06d", i) }

	stop := make(chan struct{})
	var (
		syncWG   sync.WaitGroup
		syncErrs []error
		syncs    int
	)
	syncWG.Add(1)
	go func() {
		defer syncWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			syncs++
			if err := l.Sync(); err != nil {
				syncErrs = append(syncErrs, err)
			}
		}
	}()

	var lastHS *raftpb.HardState
	for i := uint64(1); i <= total; i++ {
		var h *raftpb.HardState
		if i%100 == 0 {
			h = hs(2, 1, i)
			lastHS = h
		}
		if err := l.Append(h, []*raftpb.Entry{ent(i, 2, data(i))}, false); err != nil {
			close(stop)
			syncWG.Wait()
			t.Fatalf("Append(%d) 失败: %v", i, err)
		}
	}
	close(stop)
	syncWG.Wait()

	if err := errors.Join(syncErrs...); err != nil {
		t.Fatalf("并发 Sync 出错（共 %d 次 Sync、%d 次失败）: %v", syncs, len(syncErrs), err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) <= 50 {
		t.Fatalf("段数 = %d; want > 50（压测须高频轮转才与刷盘撞车）", len(segs))
	}
	t.Logf("段数 %d，并发 Sync %d 次", len(segs), syncs)

	collided := rec.countMsg(slog.LevelDebug, "Sync 句柄被轮转关闭")
	if collided == 0 {
		t.Fatal("压测期间没有一次 Sync 与轮转撞车，未覆盖「被轮转关闭」分支")
	}
	t.Logf("Sync 与轮转撞车 %d 次", collided)

	_, gotHS, gotEnts, err := Open(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(gotEnts) != total {
		t.Fatalf("重开条目数 = %d; want %d", len(gotEnts), total)
	}
	for k, e := range gotEnts {
		want := uint64(k + 1)
		if e.GetIndex() != want || string(e.GetData()) != data(want) {
			t.Fatalf("第 %d 条 = index %d data %q; want index %d data %q",
				k, e.GetIndex(), e.GetData(), want, data(want))
		}
	}
	if gotHS.GetTerm() != lastHS.GetTerm() || gotHS.GetVote() != lastHS.GetVote() || gotHS.GetCommit() != lastHS.GetCommit() {
		t.Fatalf("重开 HS = %v; want %v", gotHS, lastHS)
	}
}

// countMsg 统计指定级别、消息包含 sub 的记录条数。
func (h *recordHandler) countMsg(lv slog.Level, sub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == lv && strings.Contains(r.Message, sub) {
			n++
		}
	}
	return n
}
