.PHONY: build build-go web test test-cluster test-web proto e2e soak soak-e2e

# set -o pipefail 是 bash 特性，Makefile 默认的 /bin/sh 在部分平台上不支持。
# 测试目标依赖它保住 go test 的退出码，所以整个 Makefile 统一用 bash。
SHELL := /bin/bash

# 默认构建包含控制台：单二进制「启动即见一切」是产品承诺的一半
build: web build-go

# 只编 Go，用于后端迭代时跳过前端构建
build-go:
	go build -o sq ./cmd/sq

# Vite emptyOutDir 每次构建都会清空 web/dist，连带删掉 .gitkeep；
# go:embed all:dist 要求目录内至少有一个文件，干净克隆后没有 .gitkeep 会导致编译失败，
# 因此每次构建后 touch 重建占位文件。
web:
	cd web && npm ci && npm run build
	touch web/dist/.gitkeep

# 测试输出纪律（B12 教训，别改回去）：
#   ① 完整输出落文件，永不过 tail/head——2026-08-11 一次 `go test ./... | tail -60`
#      让 Pebble 噪声把唯一那行 `--- FAIL: TestXxx` 挤出了窗口，失败用例至今不可考。
#      要截，也先落完整文件再从文件里截。
#   ② set -o pipefail 保住 go test 的退出码——同一条管道当时把退出码换成了
#      tail 的 0，那次失败差点被整个漏掉。
#   ③ -timeout 显式写出（与 Go 默认值相同），让它是个可见的旋钮而非隐式默认。
test:
	set -o pipefail; go test -timeout 10m ./... 2>&1 | tee test-output.log

# internal/cluster 专用入口：该包有一次未复现的偶发失败（B12）。
#   -count=1  防测试缓存给出假绿——追偶发时缓存命中等于没跑。
#   -timeout 5m 该包全量约 72–85s，5m 是约 3.5 倍余量；真挂死时比默认的 10m
#              早 5 分钟触发栈转储。
test-cluster:
	set -o pipefail; go test -timeout 5m -count=1 ./internal/cluster/... 2>&1 | tee test-cluster.log

test-web:
	cd web && npm run test

proto:
	buf generate

e2e:
	cd test/e2e && go test -tags e2e -count=1 ./...

# 写入 soak 长跑（默认 10 分钟，16 队列/64 并发/真实 fsync）。
# SQ_SOAK_DURATION=2m 缩短；SQ_SOAK_DIR=/path 指定真实磁盘目录
# （默认 TempDir 在部分机器上落 tmpfs，量不到真实 fsync）。
soak:
	SQ_SOAK=1 go test ./internal/core/produce/ -run TestSoak -v -timeout 30m

# 端到端 soak（默认 10 分钟，16 队列 / 64 producer / 16 consumer / 真实 fsync）。
# 判定：ack_rate 均值 ≥ produce_rate 均值的 80%，backlog 不单调增长。
# SQ_SOAK_DURATION=2m 缩短；SQ_SOAK_DIR=/path 指定真实磁盘目录。
soak-e2e:
	SQ_SOAK=1 go test ./internal/core/deliver/ -run TestSoakE2E -v -timeout 30m