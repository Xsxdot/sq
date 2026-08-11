// frame_test.go 验证记录帧的编解码与损坏判定。
// 职责：roundtrip、多帧顺序解码、torn 判定（长度不足/CRC 不符）。
// 边界：不涉文件 I/O（segment 层的事）。
package seglog

import (
	"bytes"
	"errors"
	"testing"
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

func TestFrameRejectsInsaneLength(t *testing.T) {
	// 长度字段被写花成天文数字：必须判 torn，不得按 len 分配内存
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 1}
	if _, _, _, err := readFrame(buf); !errors.Is(err, errTornFrame) {
		t.Fatalf("疯长度应判 errTornFrame，得到 %v", err)
	}
}
