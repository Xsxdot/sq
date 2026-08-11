// Package seglog 提供每 raft 组一份的分段追加日志（B2 spec）。
//
// 职责：
//   - 记录帧编解码（CRC32C 校验，torn write 可判定）
//   - 段文件的追加/轮转/fsync 与启动扫描恢复
//   - 按段回收（截断 = 删整段文件）
//
// 边界：
//   - 不理解 raft 语义之外的内容：只存 Entry/HardState 的 protobuf 字节
//   - 不管锚点/applied/成员表——那些留在 Pebble（spec §3 职责划分）
//   - 运行期不提供随机读：raft 读日志走 MemoryStorage 双记账
package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// 记录类型（帧的 1B type 字段）。
const (
	recEntry     byte = 1 // payload = raftpb.Entry 的 protobuf 字节
	recHardState byte = 2 // payload = raftpb.HardState 的 protobuf 字节
)

// frameHeader = 4B len + 4B CRC。len = 1B type + payload 长度。
const frameHeaderLen = 8

// maxFrameLen 单帧上限：Entry 载荷上限（4MiB 消息体 + 头）远小于此；
// 超过即认定长度字段被写花（torn），拒绝按其分配内存。
const maxFrameLen = 64 << 20

// errTornFrame 表示缓冲区头部不构成一条完整有效帧——尾部截断、CRC
// 不符、长度字段疯掉都归此类。扫描层据此判定「末段 torn tail」。
var errTornFrame = errors.New("seglog: torn frame")

// castagnoli 与 Pebble/iSCSI 同款多项式，硬件加速普遍。
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// appendFrame 把一条记录帧追加到 dst 并返回新切片（append 语义）。
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	var hdr [frameHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(1+len(payload)))
	crc := crc32.Update(0, castagnoli, []byte{typ})
	crc = crc32.Update(crc, castagnoli, payload)
	binary.BigEndian.PutUint32(hdr[4:8], crc)
	dst = append(dst, hdr[:]...)
	dst = append(dst, typ)
	return append(dst, payload...)
}

// readFrame 从 buf 头部解一帧。
//
// 返回：
//   - typ/payload: 帧内容（payload 是 buf 的子切片，调用方需要长期持有时自行拷贝）
//   - n: 本帧消费的字节数（含头）
//   - err: errTornFrame 表示头部不构成完整有效帧
func readFrame(buf []byte) (typ byte, payload []byte, n int, err error) {
	if len(buf) < frameHeaderLen {
		return 0, nil, 0, errTornFrame
	}
	ln := binary.BigEndian.Uint32(buf[0:4])
	if ln == 0 || ln > maxFrameLen || int(ln) > len(buf)-frameHeaderLen {
		return 0, nil, 0, errTornFrame
	}
	body := buf[frameHeaderLen : frameHeaderLen+int(ln)]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(buf[4:8]) {
		return 0, nil, 0, errTornFrame
	}
	return body[0], body[1:], frameHeaderLen + int(ln), nil
}
