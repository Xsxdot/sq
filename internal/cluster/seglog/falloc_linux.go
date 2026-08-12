//go:build linux

// falloc_linux.go 提供段文件的 Linux 平台能力：预分配与数据同步。
//
// 职责：
//   - 把段文件一次性扩到定长并真实分配磁盘块（fallocate mode 0）
//   - 提供只同步数据、不同步 inode 元数据的落盘（fdatasync）
//
// 边界：
//   - 不理解段/帧语义，只对 *os.File 操作
//   - 不决定「什么时候该用 datasync」——那个前提由调用方（Log）持有
package seglog

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocate 把 f 一次性扩到 size 字节并真实分配磁盘块（Linux fallocate）。
//
// 参数：
//   - f: 已打开的可写文件
//   - size: 目标字节数；已有内容原样保留
//
// 返回：
//   - allocated: true 表示预分配生效，此后写入不再改变文件大小，调用方
//     可以安全地用 datasync 代替 fsync；false + nil error 表示底层文件
//     系统不支持，调用方必须退回 fsync——这不是错误，是能力探测的正常结果
//   - err: 真 I/O 错误（ENOSPC 等）
//
// 注意：
//   - 必须用 mode 0（真扩 st_size），**绝不能用 FALLOC_FL_KEEP_SIZE**。
//     后者保持 st_size 不变，写入时文件大小照样增长、元数据日志照付，
//     整个方案的收益归零。
func preallocate(f *os.File, size int64) (bool, error) {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil {
		return true, nil
	}
	// ENOTSUP：文件系统不支持 fallocate（网络文件系统、部分 overlayfs）。
	// EINVAL：部分文件系统对 mode 0 也报这个而不是 ENOTSUP。
	// 两者都归入「不支持」而非错误——功能可以退回 fsync 继续跑。
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EINVAL) {
		return false, nil
	}
	return false, fmt.Errorf("seglog: fallocate %s 到 %d 字节失败: %w", f.Name(), size, err)
}

// datasync 只把数据落盘，不同步 inode 元数据（Linux fdatasync）。
//
// 前提（调用方负责）：文件已预分配，本次写入不改变文件大小。否则「文件
// 长了」这件事可能不落盘，掉电后已写入的尾部字节读不回来。
func datasync(f *os.File) error {
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("seglog: fdatasync %s 失败: %w", f.Name(), err)
	}
	return nil
}
