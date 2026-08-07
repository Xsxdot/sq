// Package web 承载控制台的构建产物。
//
// 职责：
//   - 通过 go:embed 把 Vite 产物打进二进制，实现「单文件启动即见一切」
//
// 边界：
//   - 不含任何 Go 逻辑；提供服务的是 internal/admin/web.go
//   - dist/ 由 `make web` 生成且不入库（只留一个 .gitkeep 让 go:embed 有东西可嵌）；
//     未构建时二进制照样能起，控制台路径会返回「请先 make web」的提示
package web

import "embed"

// Dist 控制台构建产物。all: 前缀保证以 . 或 _ 开头的文件（如 .gitkeep、
// .vite 资源）也被嵌入，否则未构建时 go:embed 会因为匹配不到文件而编译失败。
//
//go:embed all:dist
var Dist embed.FS
