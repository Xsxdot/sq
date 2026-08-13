// clientid_test.go 覆盖客户端标识提取（clientid.go）：不同 metadata 输入下
// clientIDFrom 的取值行为。不启 gRPC，纯函数单测。
//
// 职责：
//   - 验证无 metadata / 无该头 / 头值为空 / 正常取值四条分支
//
// 边界：
//   - 不覆盖会话存活索引（那是 sessions_test.go 的职责）
package rpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestClientIDFrom(t *testing.T) {
	cases := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"无 metadata", context.Background(), ""},
		{"无该头", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("x-mq-language", "golang")), ""},
		{"头值为空", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(clientIDHeaderKey, "")), ""},
		{"正常取值", metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(clientIDHeaderKey, "cli-abc")), "cli-abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientIDFrom(tc.ctx); got != tc.want {
				t.Fatalf("clientIDFrom = %q，期望 %q", got, tc.want)
			}
		})
	}
}
