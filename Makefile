.PHONY: build build-go web test test-web proto e2e

# 默认构建包含控制台：单二进制「启动即见一切」是产品承诺的一半
build: web build-go

# 只编 Go，用于后端迭代时跳过前端构建
build-go:
	go build -o sq ./cmd/sq

web:
	cd web && npm ci && npm run build

test:
	go test ./...

test-web:
	cd web && npm run test

proto:
	buf generate

e2e:
	go test -tags e2e -count=1 ./test/e2e/...