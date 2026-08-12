//go:build !linux

// falloc_other.go 提供段文件平台能力在非 Linux 平台的退化实现。
//
// 职责：
//   - 让 seglog 在非 Linux 平台以「不预分配 + 完整 fsync」的形态正常工作
//
// 边界：
//   - 不尝试用 darwin 的 F_PREALLOCATE 模拟：它只预留磁盘块、不扩 st_size，
//     写入时文件大小照样增长，拿不到「同步落盘不碰元数据」这个收益——
//     为它引入一条只在开发机上跑的独立路径，是纯粹的风险
package seglog

import "os"

// preallocate 在非 Linux 平台不做预分配，恒定返回未生效。
//
// 调用方据此把 prealloc 标志置 false、落盘继续走 File.Sync()，行为与本
// 特性落地之前完全一致。开发与 -race 全量测试跑在 macOS 上，走的就是
// 这条路径。
func preallocate(f *os.File, size int64) (bool, error) { return false, nil }

// datasync 在非 Linux 平台退回完整 fsync。
//
// 正常流程下本函数不会被调用（prealloc == false 时调用方直接走
// File.Sync）。保留实现是为了让两个平台的函数集合完全一致，调用方无需
// 任何条件编译。
func datasync(f *os.File) error { return f.Sync() }
