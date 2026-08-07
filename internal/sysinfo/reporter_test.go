package sysinfo

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newTestReporter(t *testing.T, dir string, watermark int, blocked *atomic.Bool) *Reporter {
	t.Helper()
	return New(dir, watermark, blocked, slog.Default())
}

func TestSnapshotReportsWriteBlockedAndWatermark(t *testing.T) {
	blocked := &atomic.Bool{}
	r := newTestReporter(t, t.TempDir(), 85, blocked)

	if s := r.Snapshot(); s.WriteBlocked {
		t.Fatal("初始不该处于拒写状态")
	}
	blocked.Store(true)
	s := r.Snapshot()
	if !s.WriteBlocked {
		t.Fatal("拒写开关置位后 Snapshot 应反映出来")
	}
	if s.WatermarkPercent != 85 {
		t.Fatalf("水位线应为 85，得到 %d", s.WatermarkPercent)
	}
}

func TestSnapshotReportsRuntimeReadings(t *testing.T) {
	r := newTestReporter(t, t.TempDir(), 0, &atomic.Bool{})
	s := r.Snapshot()
	if s.GoHeapInuseBytes == 0 {
		t.Fatal("堆内存不该为 0")
	}
	if s.GoSysBytes < s.GoHeapInuseBytes {
		t.Fatalf("向 OS 申请量 %d 不该小于堆占用 %d", s.GoSysBytes, s.GoHeapInuseBytes)
	}
	if s.Goroutines < 1 {
		t.Fatalf("协程数至少为 1，得到 %d", s.Goroutines)
	}
	if s.UptimeSeconds < 0 {
		t.Fatalf("运行时长不该为负: %d", s.UptimeSeconds)
	}
}

func TestSnapshotDataDirBytesIsCached(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newTestReporter(t, dir, 0, &atomic.Bool{})

	first := r.Snapshot().DataDirBytes
	if first == nil || *first != 1000 {
		t.Fatalf("首次统计应为 1000，得到 %v", first)
	}
	// TTL 内新增文件不该被立刻看到 —— 这正是缓存存在的证据
	if err := os.WriteFile(filepath.Join(dir, "b.bin"), make([]byte, 2000), 0o644); err != nil {
		t.Fatal(err)
	}
	second := r.Snapshot().DataDirBytes
	if second == nil || *second != 1000 {
		t.Fatalf("TTL 内应返回缓存值 1000，得到 %v", second)
	}
	// 把缓存时刻推回过去，模拟 TTL 过期
	r.mu.Lock()
	r.dirAt = time.Now().Add(-2 * dirSizeTTL)
	r.mu.Unlock()

	third := r.Snapshot().DataDirBytes
	if third == nil || *third != 3000 {
		t.Fatalf("TTL 过期后应重新统计得到 3000，得到 %v", third)
	}
}

func TestSnapshotOnBadDirDegradesGracefully(t *testing.T) {
	r := newTestReporter(t, filepath.Join(t.TempDir(), "no-such-dir"), 85, &atomic.Bool{})
	s := r.Snapshot()
	if s.Disk != nil {
		t.Fatal("目录不存在时 Disk 应为 nil（不知道），而不是一组 0")
	}
	if s.DataDirBytes != nil {
		t.Fatal("目录不存在时 DataDirBytes 应为 nil")
	}
	// 但运行时读数仍然要有：磁盘探不到不该让整个端点变哑
	if s.GoHeapInuseBytes == 0 {
		t.Fatal("磁盘失败不应影响运行时读数")
	}
}

func TestWriteBlockedTolerantOfNilSwitch(t *testing.T) {
	r := New(t.TempDir(), 0, nil, slog.Default())
	if r.WriteBlocked() {
		t.Fatal("拒写开关为 nil 时应视为未拒写")
	}
}
