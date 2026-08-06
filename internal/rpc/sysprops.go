// sysprops.go: SystemProperties 里"sq 不解释、只透传"的字段在协议枚举与 core
// 协议无关表示之间的双向映射。
//
// 职责：
//   - body_encoding、body_digest 的 pb ↔ core 互转
//
// 边界：
//   - 只做类型映射，不做任何校验：sq 不按 encoding 解压 Body，也不重算或核对
//     digest（那是生产者与消费者之间端到端的事）
//
// 为什么两个方向写在同一个文件里：这几个字段最初就是因为"写方向在 send.go、
// 读方向在 receive.go"而各自漏掉了一半——写进去了没读出来，或者压根两边都没接。
// 把一对互逆函数放在相邻位置，新增一种编码时漏掉反向分支会立刻显眼。
package rpc

import (
	"github.com/xushixin/sq/internal/core"
	pb "github.com/xushixin/sq/internal/rpc/pb/apache/rocketmq/v2"
)

// bodyEncodingToCore 协议枚举 → core 表示。第二个返回值为 false 表示生产者
// 声明了一种本版本不认识的编码。
//
// 调用方必须把 false 当回事：sq 把消息体原样存下、投递时按 core 表示回填协议
// 字段，一旦这里把未知编码悄悄降级成"未声明"，消费端就会拿到一段自己不知道
// 该怎么解释的字节（比如压缩过的 Body 被当成明文交给应用），属于静默的数据
// 损坏。降级本身无法避免（sq 存不下自己不认识的 token），但必须留下痕迹。
func bodyEncodingToCore(e pb.Encoding) (core.BodyEncoding, bool) {
	switch e {
	case pb.Encoding_ENCODING_UNSPECIFIED:
		return core.BodyEncodingUnspecified, true
	case pb.Encoding_IDENTITY:
		return core.BodyEncodingIdentity, true
	case pb.Encoding_GZIP:
		return core.BodyEncodingGzip, true
	default:
		return core.BodyEncodingUnspecified, false
	}
}

// bodyEncodingToPB core 表示 → 协议枚举，与 bodyEncodingToCore 互逆。
// 第二个返回值为 false 表示盘上存着一个本版本不认识的 token（正常路径下不会
// 发生：能落盘的 token 只可能来自上面那个函数；出现即说明数据被外部改写，
// 或者本文件的两个方向被改得不再对称）。
func bodyEncodingToPB(e core.BodyEncoding) (pb.Encoding, bool) {
	switch e {
	case core.BodyEncodingUnspecified:
		return pb.Encoding_ENCODING_UNSPECIFIED, true
	case core.BodyEncodingIdentity:
		return pb.Encoding_IDENTITY, true
	case core.BodyEncodingGzip:
		return pb.Encoding_GZIP, true
	default:
		return pb.Encoding_ENCODING_UNSPECIFIED, false
	}
}

// digestToCore 协议 Digest → core 表示；nil 进 nil 出（生产者没带校验和）。
//
// 与 encoding 不同，未知算法这里直接降级成"未声明"且不额外报告：sq 从不校验
// digest，消费端拿到 UNSPECIFIED 只会跳过校验，是"少做一次可选的完整性检查"
// 这种安全方向的降级；而未知 encoding 会让 Body 被按错误的方式解释，性质完全
// 不同，所以只有后者需要 ok 返回值。
func digestToCore(d *pb.Digest) *core.BodyDigest {
	if d == nil {
		return nil
	}
	var t core.DigestType
	switch d.GetType() {
	case pb.DigestType_CRC32:
		t = core.DigestTypeCRC32
	case pb.DigestType_MD5:
		t = core.DigestTypeMD5
	case pb.DigestType_SHA1:
		t = core.DigestTypeSHA1
	default:
		t = core.DigestTypeUnspecified
	}
	return &core.BodyDigest{Type: t, Checksum: d.GetChecksum()}
}

// digestToPB core 表示 → 协议 Digest，与 digestToCore 互逆；nil 进 nil 出。
func digestToPB(d *core.BodyDigest) *pb.Digest {
	if d == nil {
		return nil
	}
	var t pb.DigestType
	switch d.Type {
	case core.DigestTypeCRC32:
		t = pb.DigestType_CRC32
	case core.DigestTypeMD5:
		t = pb.DigestType_MD5
	case core.DigestTypeSHA1:
		t = pb.DigestType_SHA1
	default:
		t = pb.DigestType_DIGEST_TYPE_UNSPECIFIED
	}
	return &pb.Digest{Type: t, Checksum: d.Checksum}
}
