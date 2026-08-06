// TagFilter：服务端 Tag 过滤（spec §5 流程 2；RocketMQ TAG 表达式子集）。
//
// 职责：
//   - 解析 "*" / "tagA" / "tagA || tagB" 三种 TAG 表达式
//   - O(1) 判定消息 tag 是否命中
//
// 边界：
//   - SQL92 属性过滤按 spec 排到 v1.1，不在此处
//   - 过滤的位点语义（跳过即永久越过）由 deliver 主流程负责，本文件只管匹配
package deliver

import (
	"fmt"
	"strings"
)

// TagFilter 不可变 tag 集合过滤器。nil 值匹配一切（等价 "*"）。
type TagFilter struct {
	tags map[string]struct{}
}

// ParseTagFilter 解析 TAG 过滤表达式。"*" 与空串返回 (nil, nil) 表示不过滤；
// 其余按 "||" 分隔为 tag 集合；出现空 token（如 "a ||"）视为非法。
func ParseTagFilter(expr string) (*TagFilter, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" || expr == "*" {
		return nil, nil
	}
	set := map[string]struct{}{}
	for _, tok := range strings.Split(expr, "||") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			return nil, fmt.Errorf("TAG 表达式含空 tag: %q", expr)
		}
		set[tok] = struct{}{}
	}
	return &TagFilter{tags: set}, nil
}

// Match 判定 tag 是否命中。区分大小写（与 RocketMQ 一致）。
func (f *TagFilter) Match(tag string) bool {
	if f == nil {
		return true
	}
	_, ok := f.tags[tag]
	return ok
}
