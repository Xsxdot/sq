// frame_test.go 验证记录帧的编解码与损坏判定。
// 职责：roundtrip、多帧顺序解码、torn 判定（长度不足/CRC 不符）。
// 边界：不涉文件 I/O（segment 层的事）。
package seglog

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

func TestFrameRoundTrip(t *testing.T) {
	buf := appendFrame(nil, recEntry, []byte("payload-a"))
	buf = appendFrame(buf, recHardState, []byte("payload-b"))
	typ, payload, n, err := readFrame(buf)
	if err != nil || typ != recEntry || !bytes.Equal(payload, []byte("payload-a")) {
		t.Fatalf("第一帧 = %d,%q,%v; want recEntry,payload-a,nil", typ, payload, err)
	}
	typ, payload, _, err = readFrame(buf[n:])
	if err != nil || typ != recHardState || !bytes.Equal(payload, []byte("payload-b")) {
		t.Fatalf("第二帧 = %d,%q,%v; want recHardState,payload-b,nil", typ, payload, err)
	}
}

func TestFrameTornDetection(t *testing.T) {
	full := appendFrame(nil, recEntry, []byte("payload"))
	// 截尾模拟掉电 torn write：任何前缀都必须判 torn，不 panic 不误读
	for cut := 0; cut < len(full); cut++ {
		if _, _, _, err := readFrame(full[:cut]); !errors.Is(err, errTornFrame) {
			t.Fatalf("截到 %d 字节应判 errTornFrame，得到 %v", cut, err)
		}
	}
	// CRC 损坏（翻转 payload 一位）同判 torn
	bad := append([]byte(nil), full...)
	bad[len(bad)-1] ^= 0x01
	if _, _, _, err := readFrame(bad); !errors.Is(err, errTornFrame) {
		t.Fatalf("CRC 损坏应判 errTornFrame，得到 %v", err)
	}
}

// TestAppendFrameMarshalMatchesAppendFrame 锁死盘上格式：Append 热路径
// 迁移到 appendFrameMarshal（MarshalAppend 直接续写 buf）后，写出的帧
// 必须与旧路径「proto.Marshal 到临时切片 + appendFrame」逐字节相同——
// 帧头长度、CRC、type、payload 全部一致，段文件格式不因优化改变。
func TestAppendFrameMarshalMatchesAppendFrame(t *testing.T) {
	term, vote, commit, index := uint64(7), uint64(3), uint64(42), uint64(43)
	hs := &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	ent := &raftpb.Entry{Term: &term, Index: &index,
		Data: []byte("batch-repr-bytes")}
	empty := &raftpb.Entry{} // 零值消息：payload 长度为 0 的边界

	cases := []struct {
		name string
		typ  byte
		msg  proto.Message
	}{
		{"hardstate", recHardState, hs},
		{"entry", recEntry, ent},
		{"empty-entry", recEntry, empty},
	}
	for _, tc := range cases {
		payload, err := proto.Marshal(tc.msg)
		if err != nil {
			t.Fatalf("%s: 旧路径 Marshal 失败: %v", tc.name, err)
		}
		old := appendFrame(nil, tc.typ, payload)
		// 前缀非空的 dst 也要对齐（Append 里 HS 帧后续写 entry 帧的形态）
		newBuf, err := appendFrameMarshal([]byte("prefix"), tc.typ, tc.msg)
		if err != nil {
			t.Fatalf("%s: appendFrameMarshal 失败: %v", tc.name, err)
		}
		if !bytes.Equal(newBuf[len("prefix"):], old) {
			t.Fatalf("%s: 新旧路径帧字节不一致\nold=%x\nnew=%x",
				tc.name, old, newBuf[len("prefix"):])
		}
		// 回读校验：新路径帧必须能被 readFrame 原样解出
		typ, got, _, err := readFrame(newBuf[len("prefix"):])
		if err != nil || typ != tc.typ || !bytes.Equal(got, payload) {
			t.Fatalf("%s: 新路径帧回读 = %d,%x,%v; want %d,%x,nil",
				tc.name, typ, got, err, tc.typ, payload)
		}
	}
}

func TestFrameRejectsInsaneLength(t *testing.T) {
	// 长度字段被写花成天文数字：必须判 torn，不得按 len 分配内存
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 1}
	if _, _, _, err := readFrame(buf); !errors.Is(err, errTornFrame) {
		t.Fatalf("疯长度应判 errTornFrame，得到 %v", err)
	}
}
