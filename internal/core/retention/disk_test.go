// 磁盘用量探测测试（unix 实测 statfs；数值合理性断言）。
package retention

import "testing"

func TestDiskUsedPercentSane(t *testing.T) {
	v, err := diskUsedPercent(t.TempDir())
	if err != nil {
		t.Skipf("平台不支持磁盘探测: %v", err)
	}
	if v < 0 || v > 100 {
		t.Fatalf("磁盘用量超出 [0,100]: %v", v)
	}
}
