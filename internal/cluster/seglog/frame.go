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

	"google.golang.org/protobuf/proto"
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
//
// 调用方：轮转时的 HardState 补写（低频路径）与 frame_test 的格式对照。
// Append 热路径已迁移到 appendFrameMarshal（免去先 Marshal 到临时切片
// 再整拷进 buf 的一次分配 + 一次拷贝），两者写出的帧逐字节相同——
// 见 TestAppendFrameMarshalMatchesAppendFrame。
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

// appendFrameMarshal 把 msg 的 protobuf 序列化直接编码为一条记录帧追加
// 到 dst 并返回新切片（append 语义），盘上格式与 appendFrame 逐字节
// 相同（回归对照见 TestAppendFrameMarshalMatchesAppendFrame）。
//
// 为什么单独存在：appendFrame 要求调用方先 proto.Marshal 出独立的
// payload 切片，每条记录多一次分配 + 一次整拷。这里先在 dst 预留
// 9B（8B 帧头 + 1B type），用 MarshalAppend 让 payload 直接续写进
// dst，再回填帧头——len = 1+payloadLen，CRC 按与 appendFrame 完全一致
// 的顺序（先 type 字节后 payload，Castagnoli 表；crc32.Update 的链式
// 结果等价于对拼接区间一次 Checksum）。
//
// 失败时返回 (原 dst 截回追加前的长度, err)：预留头不残留，调用方
// fail-stop 即可，无需回滚。
func appendFrameMarshal(dst []byte, typ byte, msg proto.Message) ([]byte, error) {
	base := len(dst)
	// 预留 8B 帧头 + 1B type；帧头此刻为零值占位，payload 长度要等
	// MarshalAppend 写完才知道，之后回填
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, typ)
	out, err := proto.MarshalOptions{}.MarshalAppend(dst, msg)
	if err != nil {
		return dst[:base], err
	}
	payloadLen := len(out) - base - frameHeaderLen - 1
	binary.BigEndian.PutUint32(out[base:base+4], uint32(1+payloadLen))
	// CRC 覆盖 type 字节 + payload 区间（即帧头之后的全部本帧字节），
	// 与 appendFrame 的两段 Update 完全同序
	crc := crc32.Checksum(out[base+frameHeaderLen:], castagnoli)
	binary.BigEndian.PutUint32(out[base+4:base+8], crc)
	return out, nil
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
