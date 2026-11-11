// live 采集的故事级检查（SQ4.9）：
//   - S1 TestLiveMatchesFullScan：低成本采集 CollectLive 与全扫 Collect 在
//     真实装配（store/meta/produce/deliver/delay/txn）产生的随机状态上逐字段相等
//   - S2 TestSampleCostIndependentOfBacklog：采样与抓取的扫描键数不随
//     delay/half 积压量增长
//   - S3 TestMetricNamesAndLabelsUnchanged：指标族名/类型/标签/HELP 不变
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/Xsxdot/sq/internal/core"
	"github.com/Xsxdot/sq/internal/core/delay"
	"github.com/Xsxdot/sq/internal/core/deliver"
	"github.com/Xsxdot/sq/internal/core/meta"
	"github.com/Xsxdot/sq/internal/core/produce"
	"github.com/Xsxdot/sq/internal/core/txn"
	"github.com/Xsxdot/sq/internal/replication"
	"github.com/Xsxdot/sq/internal/store"
	"github.com/Xsxdot/sq/internal/sysinfo"
)

// liveFixture 单机全装配；ckpt>0 时 deliver 走检查点模式。
type liveFixture struct {
	st  *store.Store
	mt  *meta.Meta
	pr  *produce.Producer
	dl  *deliver.Deliverer
	ds  *delay.Scheduler
	tx  *txn.Manager
	rep replication.Replicator
	rt  replication.Router
}

func newLiveFixture(t *testing.T, dir string, ckpt time.Duration) *liveFixture {
	t.Helper()
	st, err := store.OpenWithOptions(dir, false, slog.Default(), store.Options{CountPrefixes: CountedPrefixes()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep := replication.NewStandalone(st)
	rt := replication.StandaloneRouter{}
	mt, err := meta.New(rep, rt, st, true, 4, 3, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	pr := produce.New(rep, rt, st, mt, slog.Default())
	var opts []deliver.Option
	if ckpt > 0 {
		opts = append(opts, deliver.WithCheckpointInterval(ckpt))
	}
	dl := deliver.New(rep, rt, st, mt, pr, slog.Default(), opts...)
	if ckpt > 0 {
		dl.Start(context.Background())
		t.Cleanup(func() { dl.Stop(context.Background()) })
	}
	return &liveFixture{st: st, mt: mt, pr: pr, dl: dl, rep: rep, rt: rt,
		ds: delay.New(rep, rt, st, pr, slog.Default()),
		tx: txn.New(rep, rt, st, pr, mt, time.Hour, 15, slog.Default())}
}

// countingSource 包装 inflight 数据源，统计 ok=true / ok=false 次数（防空过）。
type countingSource struct {
	src     InflightSource
	hit     atomic.Int64
	fallthr atomic.Int64
}

func (c *countingSource) InflightCount(g, tp string, q uint32) (int, bool) {
	n, ok := c.src.InflightCount(g, tp, q)
	if ok {
		c.hit.Add(1)
	} else {
		c.fallthr.Add(1)
	}
	return n, ok
}

type delayHandle struct {
	topic string
	due   int64
	seq   uint64
}

// drive 用种子生成一串随机业务操作（全部经真实入口）。
func (f *liveFixture) drive(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed))
	topics := []string{"ta", "tb", "tc"}
	groups := []string{"g1", "g2", "g3"}
	var recv []*core.Message
	recvGroup := map[*core.Message]string{}
	var delays []delayHandle
	var txIDs []string
	for i := 0; i < 150; i++ {
		tp := topics[r.Intn(len(topics))]
		switch x := r.Intn(100); {
		case x < 20:
			if _, err := f.pr.Append(ctx, &core.Message{Topic: tp, Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		case x < 25:
			batch := []*core.Message{{Topic: tp, Body: []byte("b1")}, {Topic: tp, Body: []byte("b2")}, {Topic: tp, Body: []byte("b3")}}
			if _, err := f.pr.AppendBatch(ctx, batch); err != nil {
				t.Fatal(err)
			}
		case x < 45:
			g := groups[r.Intn(len(groups))]
			ms, err := f.dl.Receive(ctx, g, tp, uint32(r.Intn(4)), 1+r.Intn(4), time.Hour, 0, deliver.AllPass)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range ms {
				recv = append(recv, m)
				recvGroup[m] = g
			}
		case x < 60 && len(recv) > 0:
			j := r.Intn(len(recv))
			m := recv[j]
			recv = append(recv[:j], recv[j+1:]...)
			g := recvGroup[m]
			var err error
			switch r.Intn(3) {
			case 0:
				_, err = f.dl.Ack(ctx, g, m.Topic, m.QueueID, m.Offset, m.DeliveryAttempt)
			case 1:
				_, err = f.dl.ChangeInvisible(ctx, g, m.Topic, m.QueueID, m.Offset, m.DeliveryAttempt, time.Hour)
			default:
				_, err = f.dl.ForwardToDLQ(ctx, g, m.Topic, m.QueueID, m.Offset, m.DeliveryAttempt)
			}
			if err != nil {
				t.Fatal(err)
			}
		case x < 72:
			// 远期（留在暂存区）或近期（随后到期被 Pass 搬运）
			due := time.Now().Add(time.Hour).UnixMilli()
			if r.Intn(2) == 0 {
				due = time.Now().Add(30 * time.Millisecond).UnixMilli()
			}
			m, seq, staged, err := f.pr.AppendDelay(ctx, &core.Message{Topic: tp, Body: []byte("d"), DeliverAtMs: due})
			if err != nil {
				t.Fatal(err)
			}
			if staged {
				delays = append(delays, delayHandle{m.Topic, due, seq})
			}
		case x < 77 && len(delays) > 0:
			j := r.Intn(len(delays))
			d := delays[j]
			delays = append(delays[:j], delays[j+1:]...)
			// 已被搬运的条目撤回会失败（查无此条）：两种结局都是合法状态
			_, _ = f.ds.Recall(ctx, d.topic, d.due, d.seq)
		case x < 80:
			time.Sleep(40 * time.Millisecond)
			if _, err := f.ds.Pass(ctx); err != nil {
				t.Fatal(err)
			}
		case x < 92:
			_, id, err := f.tx.Stage(ctx, &core.Message{Topic: tp, Body: []byte("h"), Transactional: true})
			if err != nil {
				t.Fatal(err)
			}
			txIDs = append(txIDs, id)
		case len(txIDs) > 0:
			j := r.Intn(len(txIDs))
			id := txIDs[j]
			txIDs = append(txIDs[:j], txIDs[j+1:]...)
			if _, err := f.tx.End(ctx, id, r.Intn(2) == 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	// 让近期延时到期并搬运一趟
	time.Sleep(40 * time.Millisecond)
	if _, err := f.ds.Pass(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLiveMatchesFullScan(t *testing.T) {
	const seeds = 20
	for _, mode := range []struct {
		name string
		ckpt time.Duration
	}{{"逐次复制", 0}, {"检查点20ms", 20 * time.Millisecond}} {
		t.Run(mode.name, func(t *testing.T) {
			var hits, falls int64
			var delayMax, halfMax int
			for seed := int64(1); seed <= seeds; seed++ {
				f := newLiveFixture(t, t.TempDir(), mode.ckpt)
				f.drive(t, seed)
				if mode.ckpt > 0 {
					time.Sleep(6 * mode.ckpt) // 静置 ≥5 个检查点间隔
				}
				want, err := Collect(f.st, f.mt)
				if err != nil {
					t.Fatal(err)
				}
				// leader 视角（本节点已装载各队列）
				src := &countingSource{src: f.dl}
				d0, h0 := scanKeys(famDelay), scanKeys(famHalf)
				got, err := CollectLive(f.st, f.mt, src)
				if err != nil {
					t.Fatal(err)
				}
				if dd, dh := scanKeys(famDelay)-d0, scanKeys(famHalf)-h0; dd != 0 || dh != 0 {
					t.Fatalf("seed=%d CollectLive 扫了 delay/half 键（%d/%d）", seed, dd, dh)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed=%d leader 视角不等:\nlive=%+v\nscan=%+v", seed, got, want)
				}
				// follower/重启视角：新的 Deliverer 未装载任何队列，全部 ok=false 回落
				fresh := deliver.New(f.rep, f.rt, f.st, f.mt, f.pr, slog.Default())
				fsrc := &countingSource{src: fresh}
				got2, err := CollectLive(f.st, f.mt, fsrc)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got2, want) {
					t.Fatalf("seed=%d 回落视角不等:\nlive=%+v\nscan=%+v", seed, got2, want)
				}
				if fsrc.hit.Load() != 0 {
					t.Fatalf("seed=%d 未装载的 Deliverer 不应 ok=true", seed)
				}
				hits += src.hit.Load()
				falls += fsrc.fallthr.Load()
				delayMax = max(delayMax, want.DelayDepth)
				halfMax = max(halfMax, want.HalfDepth)
			}
			t.Logf("seeds=%d ok=true 命中=%d 回落=%d delay 深度最大=%d half 最大=%d", seeds, hits, falls, delayMax, halfMax)
			if hits == 0 || falls == 0 || delayMax == 0 || halfMax == 0 {
				t.Fatalf("防空过：命中=%d 回落=%d delayMax=%d halfMax=%d 均须 >0", hits, falls, delayMax, halfMax)
			}
		})
	}
}

// seedBacklog 直接写 delay/half 积压（值 1KB，模拟真实消息体）。
func seedBacklog(t *testing.T, st *store.Store, nDelay, nHalf int) {
	t.Helper()
	val := make([]byte, 1024)
	b := st.NewBatch()
	n := 0
	flush := func() {
		if err := st.Apply(b); err != nil {
			t.Fatal(err)
		}
		b = st.NewBatch()
		n = 0
	}
	far := time.Now().Add(24 * time.Hour).UnixMilli()
	for i := 0; i < nDelay; i++ {
		b.Set(store.DelayKey(far, uint64(i)), val)
		if n++; n == 5000 {
			flush()
		}
	}
	for i := 0; i < nHalf; i++ {
		b.Set(store.HalfKey(far, fmt.Sprintf("tx-%08d", i)), val)
		if n++; n == 5000 {
			flush()
		}
	}
	flush()
}

func TestSampleCostIndependentOfBacklog(t *testing.T) {
	type reading struct {
		sample, gather, full uint64
		sampleDur, fullDur   time.Duration
	}
	run := func(name string, nDelay, nHalf int) reading {
		f := newLiveFixture(t, t.TempDir(), 0)
		ctx := context.Background()
		// 「其他数据」固定：同一份写入与消费
		for i := 0; i < 200; i++ {
			if _, err := f.pr.Append(ctx, &core.Message{Topic: fmt.Sprintf("t%d", i%3), Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		for q := uint32(0); q < 4; q++ {
			if _, err := f.dl.Receive(ctx, "g", "t0", q, 5, time.Hour, 0, deliver.AllPass); err != nil {
				t.Fatal(err)
			}
		}
		seedBacklog(t, f.st, nDelay, nHalf)
		sp := NewSampler(f.st, f.mt, f.dl, 0, slog.Default())
		reg := NewRegistry(f.st, f.mt, nil, nil, nil, nil, f.dl, sp, slog.Default())
		var rd reading
		before := scanKeysTotal()
		start := time.Now()
		sp.tick()
		rd.sampleDur = time.Since(start)
		rd.sample = scanKeysTotal() - before
		before = scanKeysTotal()
		if _, err := reg.Gather(); err != nil {
			t.Fatal(err)
		}
		rd.gather = scanKeysTotal() - before
		before = scanKeysTotal()
		start = time.Now()
		if _, err := Collect(f.st, f.mt); err != nil {
			t.Fatal(err)
		}
		rd.fullDur = time.Since(start)
		rd.full = scanKeysTotal() - before
		t.Logf("数据集 %s（delay=%d half=%d）：采样扫描键数=%d（%v） 抓取增量=%d  全扫 Collect 扫描键数=%d（%v）",
			name, nDelay, nHalf, rd.sample, rd.sampleDur, rd.gather, rd.full, rd.fullDur)
		return rd
	}
	a := run("A", 10, 10)
	b := run("B", 100000, 10000)
	if a.sample != b.sample {
		t.Fatalf("采样扫描键数随积压变化：A=%d B=%d", a.sample, b.sample)
	}
	if a.gather != 0 || b.gather != 0 {
		t.Fatalf("已有快照时抓取不应扫描：A=%d B=%d", a.gather, b.gather)
	}
	if b.full < a.full+100000 {
		t.Fatalf("正向对照失效：全扫 B=%d 应比 A=%d 多 ≥10 万", b.full, a.full)
	}
}

func TestMetricNamesAndLabelsUnchanged(t *testing.T) {
	f := newLiveFixture(t, t.TempDir(), 0)
	ctx := context.Background()
	if _, err := f.pr.Append(ctx, &core.Message{Topic: "t1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.dl.Receive(ctx, "g1", "t1", 0, 1, time.Hour, 0, deliver.AllPass); err != nil {
		t.Fatal(err)
	}
	sys := sysinfo.New(t.TempDir(), 0, &atomic.Bool{}, slog.Default())
	sp := NewSampler(f.st, f.mt, f.dl, 0, slog.Default())
	fs := fakeFilterStats{m: map[deliver.FilterSkipKey]uint64{{Topic: "t1", Group: "g1", Reason: "tag_miss"}: 1}}
	reg := NewRegistry(f.st, f.mt, sys, f.tx, &fakeConns{n: 2}, fs, f.dl, sp, slog.Default())
	sp.tick()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	type fam struct {
		typ    dto.MetricType
		labels string
		help   string
	}
	want := map[string]fam{
		"sq_topics":                       {dto.MetricType_GAUGE, "", "已注册 topic 数"},
		"sq_groups":                       {dto.MetricType_GAUGE, "", "已注册消费组数"},
		"sq_delay_depth":                  {dto.MetricType_GAUGE, "", "延时暂存区未到期条数"},
		"sq_half_messages":                {dto.MetricType_GAUGE, "", "半消息暂存区待回查条数"},
		"sq_topic_messages_written_total": {dto.MetricType_COUNTER, "topic", "topic 累计写入消息条数（各队列 offset 计数器之和）"},
		"sq_group_pending_messages":       {dto.MetricType_GAUGE, "group,topic", "消费组视角已写入未拉取的消息数"},
		"sq_group_inflight_messages":      {dto.MetricType_GAUGE, "group,topic", "已投递未确认的消息数"},
		"sq_txn_checks_total":             {dto.MetricType_COUNTER, "", "事务回查累计排期次数（含下发失败的轮次）"},
		"sq_txn_dropped_total":            {dto.MetricType_COUNTER, "", "事务回查超限累计丢弃条数"},
		"sq_connections":                  {dto.MetricType_GAUGE, "", "已完成 Settings 协商的客户端连接数"},
		"sq_filter_skipped_total":         {dto.MetricType_COUNTER, "group,reason,topic", "订阅过滤累计跳过消息数，按跳过原因分桶（tag_miss/sql_false/sql_unknown）"},
		"sq_disk_used_percent":            {dto.MetricType_GAUGE, "", "数据目录所在文件系统已用百分比（与 df 同口径）"},
		"sq_disk_free_bytes":              {dto.MetricType_GAUGE, "", "数据目录所在文件系统的非特权可用字节数"},
		"sq_data_dir_bytes":               {dto.MetricType_GAUGE, "", "数据目录占用字节数（TTL 缓存，最多滞后 60s）"},
		"sq_write_blocked":                {dto.MetricType_GAUGE, "", "磁盘水位拒写开关：1=拒写保读中，0=正常写入"},
		"sq_store_apply_duration_seconds": {dto.MetricType_HISTOGRAM, "", "store 批次提交耗时（含 fsync）"},
	}
	got := map[string]fam{}
	var goSeen, procSeen bool
	for _, mf := range mfs {
		name := mf.GetName()
		switch {
		case strings.HasPrefix(name, "go_"):
			goSeen = true
			continue
		case strings.HasPrefix(name, "process_"):
			procSeen = true
			continue
		}
		var ls []string
		if len(mf.GetMetric()) > 0 {
			for _, lp := range mf.GetMetric()[0].GetLabel() {
				ls = append(ls, lp.GetName())
			}
		}
		sort.Strings(ls)
		got[name] = fam{mf.GetType(), strings.Join(ls, ","), mf.GetHelp()}
	}
	if !goSeen || !procSeen {
		t.Fatalf("go_*/process_* 指标缺失：go=%v process=%v", goSeen, procSeen)
	}
	if !reflect.DeepEqual(got, want) {
		for k, w := range want {
			if g, ok := got[k]; !ok || g != w {
				t.Errorf("指标 %s：得 %+v（存在=%v），应 %+v", k, g, ok, w)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Errorf("多出指标 %s", k)
			}
		}
		t.FailNow()
	}
}
