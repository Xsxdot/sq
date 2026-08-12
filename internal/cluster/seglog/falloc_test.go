// falloc_test.go 验证段文件的平台抽象层：预分配与数据同步。
// 职责：预分配成功时文件大小达标、预分配后写入可读回、datasync 不报错。
// 边界：不测 Log 的任何行为（seglog_test.go 覆盖）；不断言具体系统调用
//
//	——那是平台实现细节，只断言可观测的契约。
package seglog

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPreallocateReportsCapabilityAndKeepsData 预分配后：若报告成功则文件
// 大小必须达到请求值；无论成功与否，写入的数据都必须能原样读回。
//
// 两平台都跑：Linux 上 allocated=true 走预分配断言，macOS 上
// allocated=false 只走数据完整性断言——后者才是「退回 fsync 也不能坏事」
// 这条约束的守门人。
func TestPreallocateReportsCapabilityAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const size = 1 << 20
	allocated, err := preallocate(f, size)
	if err != nil {
		t.Fatalf("preallocate 返回错误: %v", err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if allocated && fi.Size() != size {
		t.Fatalf("预分配报告成功但文件大小 = %d; want %d", fi.Size(), size)
	}
	if !allocated && fi.Size() != 0 {
		t.Fatalf("预分配报告未生效但文件大小 = %d; want 0（不得有副作用）", fi.Size())
	}

	want := []byte("hello seglog")
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := datasync(f); err != nil {
		t.Fatalf("datasync 返回错误: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("读回 %q; want %q", got, want)
	}
}

// TestPreallocateIsIdempotentOverExistingData 对已有内容的文件再次预分配
// 不得破坏已写入的字节——Open 重启后补分配走的正是这条路径。
func TestPreallocateIsIdempotentOverExistingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	want := []byte("existing frame bytes")
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := preallocate(f, 1<<20); err != nil {
		t.Fatalf("preallocate 返回错误: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("预分配后读回 %q; want %q（已有内容被破坏）", got, want)
	}
}
