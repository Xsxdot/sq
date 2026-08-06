.PHONY: build test proto e2e
build:
	go build -o sq ./cmd/sq
test:
	go test ./...
e2e:
	go test -tags e2e -count=1 ./test/e2e/...
proto:
	buf generate
