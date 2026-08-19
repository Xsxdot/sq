package metrics

import (
	"log/slog"
	"testing"
	"time"

	"github.com/Xsxdot/sq/internal/store"
)

// newSampleAt 造一个只带全局读数的采样点，用于归约逻辑的单元测试。
func newSampleAt(tsMs int64, written, pending uint64, inflight, delay int) Sample {
	return Sample{TsMs: tsMs, Written: written, Pending: pending,
		Inflight: inflight, DelayDepth: delay,
		TopicWritten: map[string]uint64{}, GTPending: map[GroupTopic]uint64{},
		GTInflight: map[GroupTopic]int{}, GTConsumed: map[GroupTopic]uint64{}}
}

func TestRingKeepsLastHourAndDerivesQPS(t *testing.T) {
	s := &Sampler{ring: make([]Sample, ringSize)}
	base := int64(1754480400000)
	// 每 5 秒 +500 条：速率恒为 100 msg/s
	for i := 0; i < 5; i++ {
		s.push(newSampleAt(base+int64(i)*5000, uint64(i*500), 0, 0, 0))
	}
	pts := s.Ring()
	if len(pts) != 5 {
		t.Fatalf("环内应有 5 点，得到 %d", len(pts))
	}
	// 第一点没有前驱，速率无从计算，按 0 给出
	if pts[0].QPS != 0 {
		t.Fatalf("首点 QPS 应为 0，得到 %v", pts[0].QPS)
	}
	if pts[4].QPS != 100 {
		t.Fatalf("QPS 应为 100，得到 %v", pts[4].QPS)
	}
}

func TestRingOverwritesOldest(t *testing.T) {
	s := &Sampler{ring: make([]Sample, ringSize)}
	base := int64(1754480400000)
	for i := 0; i < ringSize+10; i++ {
		s.push(newSampleAt(base+int64(i)*5000, uint64(i), 0, 0, 0))
	}
	pts := s.Ring()
	if len(pts) != ringSize {
		t.Fatalf("环满后应恒为 %d 点，得到 %d", ringSize, len(pts))
	}
	// 最老的 10 个点必须已被覆盖
	if pts[0].TsMs != base+10*5000 {
		t.Fatalf("最老点应为第 10 个样本，得到 ts=%d", pts[0].TsMs)
	}
}

func TestMinuteReductionKeepsPeaksNotAverages(t *testing.T) {
	s := &Sampler{ring: make([]Sample, ringSize)}
	base := int64(1754480400000) // 恰好落在整分钟上
	// 一分钟 12 个 5 秒样本：其中一个 5 秒窗口暴涨 5000 条（=1000 msg/s），
	// 其余每窗口 50 条（=10 msg/s）。若按分钟平均，峰值会被抹平成 ~92 msg/s。
	var written uint64
	for i := 0; i < 12; i++ {
		if i == 7 {
			written += 5000
		} else {
			written += 50
		}
		// 落后量是 gauge：第 3 个样本冲到 9000 后回落，分钟点必须记住 9000
		pending := uint64(100)
		if i == 3 {
			pending = 9000
		}
		s.push(newSampleAt(base+int64(i)*5000, written, pending, 7, 3))
	}
	mp := s.reduceMinute(base)
	if mp.QPSPeak != 1000 {
		t.Fatalf("分钟点应记住 5 秒窗口的峰值速率 1000，得到 %v", mp.QPSPeak)
	}
	if mp.PendingMax != 9000 {
		t.Fatalf("落后量应取该分钟最大值 9000，得到 %d", mp.PendingMax)
	}
	// 累计写入是单调计数器：max 恒等于分钟末值，存末值语义更清楚
	if mp.WrittenEnd != written {
		t.Fatalf("累计写入应为分钟末值 %d，得到 %d", written, mp.WrittenEnd)
	}
}

func TestHistoryRoundTripAndExpiry(t *testing.T) {
	st, err := store.Open(t.TempDir(), false, slog.Default())
	if err != nil {
		t.Fatalf("打开 store: %v", err)
	}
	defer st.Close()

	s := &Sampler{st: st, retention: time.Hour, logger: slog.Default(), ring: make([]Sample, ringSize)}
	base := int64(1754480400000)
	for i := 0; i < 5; i++ {
		mp := MinutePoint{TsMs: base + int64(i)*60000, QPSPeak: float64(i * 10),
			WrittenEnd: uint64(i * 600), PendingMax: uint64(i), InflightMax: i, DelayMax: i}
		if err := s.persist(mp); err != nil {
			t.Fatalf("落库第 %d 点: %v", i, err)
		}
	}
	pts, err := s.History(0)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(pts) != 5 {
		t.Fatalf("应读出 5 点，得到 %d", len(pts))
	}
	if pts[2].QPS != 20 {
		t.Fatalf("第 3 点 QPS 应为 20，得到 %v", pts[2].QPS)
	}

	// 过期：砍掉 base+3min 之前的全部点
	if err := s.expire(base + 3*60000); err != nil {
		t.Fatalf("expire: %v", err)
	}
	pts, err = s.History(0)
	if err != nil {
		t.Fatalf("History（过期后）: %v", err)
	}
	if len(pts) != 2 {
		t.Fatalf("过期后应剩 2 点，得到 %d", len(pts))
	}
	if pts[0].TsMs != base+3*60000 {
		t.Fatalf("过期后最老点应为 base+3min，得到 %d", pts[0].TsMs)
	}
}

func TestLastConsumeTracksCursorAdvance(t *testing.T) {
	s := &Sampler{ring: make([]Sample, ringSize), lastConsume: map[GroupTopic]int64{}}
	gt := GroupTopic{Group: "g", Topic: "t"}
	base := int64(1754480400000)

	mk := func(ts int64, consumed uint64) Sample {
		sm := newSampleAt(ts, 0, 0, 0, 0)
		sm.GTConsumed[gt] = consumed
		return sm
	}
	s.push(mk(base, 100))
	// 首次观察不算「推进」——没有前驱就无从判断，否则重启后所有组都会显示「刚刚」
	if got := s.LastConsumeMs(gt); got != 0 {
		t.Fatalf("首次观察不应记为推进，得到 %d", got)
	}
	s.push(mk(base+5000, 100)) // 没动
	if got := s.LastConsumeMs(gt); got != 0 {
		t.Fatalf("位点未动不应记为推进，得到 %d", got)
	}
	s.push(mk(base+10000, 180)) // 推进
	if got := s.LastConsumeMs(gt); got != base+10000 {
		t.Fatalf("应记录推进时刻 %d，得到 %d", base+10000, got)
	}
}
