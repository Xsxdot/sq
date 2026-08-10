//go:build !windows

package cluster

import (
	"errors"
	"testing"
)

// TestResolveBootGenUsesProvider 注入的 provider 优先于平台实现。
func TestResolveBootGenUsesProvider(t *testing.T) {
	got, ok := resolveBootGen(func() (string, error) { return "gen-a", nil }, testSlog(t))
	if !ok || got != "gen-a" {
		t.Fatalf("resolveBootGen = (%q, %v); want (\"gen-a\", true)", got, ok)
	}
}

// TestResolveBootGenUnavailable provider 报错时必须返回不可用，且**不得**
// 返回空串当作一个正常世代——空串会和另一次「读不到」比较相等，
// 于是「两次都读不到」会被误判成「机器没重启过」，安全门直接失效。
func TestResolveBootGenUnavailable(t *testing.T) {
	got, ok := resolveBootGen(func() (string, error) { return "", errors.New("boom") }, testSlog(t))
	if ok {
		t.Fatalf("resolveBootGen 在 provider 报错时 ok=true（got=%q）——读不到必须判为不可用", got)
	}
}

// TestResolveBootGenEnvOverride 环境变量覆盖生效（进程级 e2e 靠它模拟重启）。
func TestResolveBootGenEnvOverride(t *testing.T) {
	t.Setenv(bootGenOverrideEnv, "gen-forced")
	got, ok := resolveBootGen(func() (string, error) { return "gen-real", nil }, testSlog(t))
	if !ok || got != "gen-forced" {
		t.Fatalf("resolveBootGen = (%q, %v); want (\"gen-forced\", true)", got, ok)
	}
}

// TestMachineBootGenOnThisPlatform 本平台能读出一个非空、可重复的世代。
// Linux/darwin 都应成立；其它平台本用例跳过（machineBootGen 恒报错）。
func TestMachineBootGenOnThisPlatform(t *testing.T) {
	first, err := machineBootGen()
	if err != nil {
		t.Skipf("本平台不支持机器世代读取（这是允许的保守形态）: %v", err)
	}
	if first == "" {
		t.Fatal("machineBootGen 返回空串但无错误——空串不是合法世代")
	}
	second, err := machineBootGen()
	if err != nil {
		t.Fatalf("第二次读取失败: %v", err)
	}
	if first != second {
		t.Fatalf("同一次运行内两次读取不一致：%q vs %q——世代必须在一次开机内稳定", first, second)
	}
}
