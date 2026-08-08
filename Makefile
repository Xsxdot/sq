.PHONY: build build-go web test test-web proto e2e soak

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

test:
	go test ./...

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