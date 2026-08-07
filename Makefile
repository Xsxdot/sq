.PHONY: build build-go web test test-web proto e2e

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