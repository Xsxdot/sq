# 一键安装与 `sq status` 执行 ledger

| Task | 状态 | 轮次 | Commit 范围 |
|---|---|---:|---|
| 1 | 已完成 | 0 | `internal/config/config.go`, `internal/config/adminaddr.go`, `internal/config/adminaddr_test.go`, `internal/config/config_test.go` |
| 2 | 已完成 | 0 | `cmd/sq/statusview.go`, `cmd/sq/statusview_test.go` |
| 3 | 已完成 | 0 | `cmd/sq/statusfetch.go`, `cmd/sq/status.go`, `cmd/sq/statusfetch_test.go`, `cmd/sq/main.go` |
| 4 | 修复轮 1 | 0 | 修正 `deploy/quickstart_test.sh` 的同 shell 组合命令执行方式 |
| 4 | 修复轮 2 | 0 | 让骨架阶段的跨 task 全局接口变量显式通过 shellcheck |
| 4 | 已完成 | 2 | `deploy/quickstart.sh`, `deploy/quickstart_test.sh` |
| 5 | 修复轮 1 | 0 | 将无限随机管道改为有限随机块循环，消除 `tr` broken pipe |
| 5 | 修复轮 2 | 0 | 消除 `CRED_GENERATED` 与测试桩的 shellcheck 告警 |
| 5 | 已完成 | 2 | `deploy/quickstart.sh`, `deploy/quickstart_test.sh` |
| 6 | 修复轮 1 | 0 | 修正 `grep` 对 `--admin-password` 模式的选项解析 |
| 6 | 修复轮 2 | 0 | 捕获 warn 的 stderr，并消除 shellcheck 告警与备份变量赋值告警 |
| 6 | 修复轮 3 | 0 | 防止取版本、校验和、旧口令读取在 `pipefail` 下提前退出 |
| 6 | 修复轮 4 | 0 | 清理结尾提示命令行的尾随空格并保持输出格式 |
| 6 | 已完成 | 4 | `deploy/quickstart.sh`, `deploy/quickstart_test.sh` |
| 7 | 已完成 | 0 | `.github/workflows/ci.yml`, `.github/workflows/release.yml` |
| 7 | 记账 | 0 | Docker/Podman 不存在；`runuser`/`setpriv` 均受限，非 root 发布包路径未验证 |
