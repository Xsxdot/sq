package sysinfo

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDiskUsageReportsPlausibleNumbers(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("磁盘探测仅在 unix 平台可用")
	}
	d, err := DiskUsage(t.TempDir())
	if err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if d.TotalBytes == 0 {
		t.Fatal("总容量不该为 0")
	}
	if d.FreeBytes > d.TotalBytes {
		t.Fatalf("可用 %d 不该大于总量 %d", d.FreeBytes, d.TotalBytes)
	}
	if d.UsedPercent < 0 || d.UsedPercent > 100 {
		t.Fatalf("已用百分比越界: %v", d.UsedPercent)
	}
}

func TestDiskUsageOnMissingDirFails(t *testing.T) {
	if _, err := DiskUsage(filepath.Join(t.TempDir(), "no-such-dir")); err == nil {
		t.Fatal("不存在的目录应返回错误")
	}
}

func TestDirSizeSumsRegularFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(p string, n int) {
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "a.bin"), 1000)
	write(filepath.Join(sub, "b.bin"), 2000)

	got, err := dirSize(root)
	if err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if got != 3000 {
		t.Fatalf("目录大小应为 3000，得到 %d", got)
	}
}

func TestDirSizeOnMissingDirFails(t *testing.T) {
	if _, err := dirSize(filepath.Join(t.TempDir(), "no-such-dir")); err == nil {
		t.Fatal("不存在的目录应返回错误")
	}
}
