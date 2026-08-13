package delay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xushixin/sq/internal/core"
	"github.com/xushixin/sq/internal/store"
)

// deliveredIDs 扫 msg/ 区，收集指定 topic 的消息 ID。
//
// 直接用字面量 "msg/"：store 未导出 msgPrefix（keys.go 里是未导出常量），
// 本包既有的 msgCount helper 也是这么写的。
func deliveredIDs(t *testing.T, f *fixture, topic string) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	pfx := []byte("msg/")
	if err := f.st.Scan(pfx, store.PrefixUpperBound(pfx), 0, func(k, v []byte) (bool, error) {
		m, err := core.DecodeMessage(v)
		if err == nil && m.Topic == topic {
			out[m.ID] = struct{}{}
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func countTrue(bs []bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}

// U4 正常撤回 + 幂等拒绝。
func TestRecallDeletesUnexpiredEntry(t *testing.T) {
	f := newFixture(t)
	due := time.Now().Add(time.Hour).UnixMilli()
	m, seq, staged, err := f.pr.AppendDelay(context.Background(),
		&core.Message{Topic: "t-recall", Body: []byte("x"), DeliverAtMs: due})
	if err != nil || !staged {
		t.Fatalf("AppendDelay: err=%v staged=%v", err, staged)
	}

	id, err := f.sc.Recall(context.Background(), "t-recall", due, seq)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if id != m.ID {
		t.Fatalf("返回 ID=%q，期望 %q", id, m.ID)
	}
	if _, ok, err := f.st.Get(store.DelayKey(due, seq)); err != nil || ok {
		t.Fatalf("条目仍存在（ok=%v err=%v）", ok, err)
	}
	// 幂等拒绝：再撤一次必须报"不存在"，不能报成功
	if _, err := f.sc.Recall(context.Background(), "t-recall", due, seq); !errors.Is(err, ErrRecallNotFound) {
		t.Fatalf("重复撤回 err=%v，期望 ErrRecallNotFound", err)
	}
}

// U5 已过投递时间：用 putDelay 直接造条目，绕开 AppendDelay 的"已到期直通
// Append"分支（那条分支根本不会留下 delay 条目）。
func TestRecallRejectsExpiredEntry(t *testing.T) {
	f := newFixture(t)
	due := time.Now().Add(-time.Second).UnixMilli()
	m := &core.Message{Topic: "t-recall", ID: core.NewMessageID(), Body: []byte("x")}
	f.putDelay(t, 0, due, m)

	if _, err := f.sc.Recall(context.Background(), "t-recall", due, 0); !errors.Is(err, ErrRecallTooLate) {
		t.Fatalf("err=%v，期望 ErrRecallTooLate", err)
	}
}

// U6 topic 不匹配：句柄指向的条目属于别的 topic 时必须拒绝，不得跨 topic 删除。
func TestRecallRejectsTopicMismatch(t *testing.T) {
	f := newFixture(t)
	due := time.Now().Add(time.Hour).UnixMilli()
	_, seq, _, err := f.pr.AppendDelay(context.Background(),
		&core.Message{Topic: "t-real", Body: []byte("x"), DeliverAtMs: due})
	if err != nil {
		t.Fatalf("AppendDelay: %v", err)
	}
	if _, err := f.sc.Recall(context.Background(), "t-other", due, seq); !errors.Is(err, ErrRecallTopicMismatch) {
		t.Fatalf("err=%v，期望 ErrRecallTopicMismatch", err)
	}
	// 承重：拒绝之后条目必须还在——不匹配不得产生任何副作用
	if _, ok, _ := f.st.Get(store.DelayKey(due, seq)); !ok {
		t.Fatalf("topic 不匹配却把条目删了")
	}
}

// U7【承重】撤回与调度器的并发不变式。
//
// 这是 spec §3 的可执行形式，也是本条目存在的理由。断言的不变式只有一条：
// **绝不允许「撤回返回成功、消息却仍被投递」**。
// 反过来（撤回失败但消息被投递、撤回成功且消息未被投递）都是合法结果——
// 撤回本来就是在和调度器赛跑，输了不是 bug，谎报赢了才是。
//
// 它专门冲着「Scan 在 moveMu 之外」这条路径设计：due 全钉在 now+50ms，
// 撤回与到期扫描必然重叠。
//
// 关于判别力要诚实：单机档下「Scan 拷贝 → Recall 提交删除并返回 → moveOne 照陈旧 raw 入队」的窗口只有 µs 级，
// 相对 1ms 扫描周期几乎不重叠，删掉重读本用例多半仍绿——它是压力测试而非确定性判别器。
// 闸门二的结构必要性不靠本用例自证，见 moveOne 开头锁内重读注释的完整交错论证；本用例的价值是撞中即红，多跑可提高撞中概率。
func TestRecallNeverFalselySucceeds(t *testing.T) {
	f := newFixture(t)
	const n = 64
	type entry struct {
		id  string
		due int64
		seq uint64
	}
	// 与计划唯一的时序偏离：这里不再逐个 AppendDelay（每个都是一次 fsync，
	// 64 个串行 ≈ 250ms，机器慢一点就超过 due 预算导致 staged=false），而是
	// 把 64 条延时条目合成一个批次一次落盘（一次 fsync，setup≈几 ms）。
	// 条目形态与 AppendDelay 完全一致（delay/ 下键为 DelayKey(dueMs, seq)、
	// 值为完整编码消息），seq 用 0..n-1 由本测试直接分配——Recall 与调度器都
	// 只按 DelayKey 寻址，不读 delayalloc 计数器，语义无差。topic 无需预建：
	// 移入第一段走 produce.Append，EnsureTopic 的 autoCreate 会补建。
	var es []entry
	due := time.Now().Add(50 * time.Millisecond).UnixMilli()
	b := f.st.NewBatch()
	for i := 0; i < n; i++ {
		m := &core.Message{Topic: "t-race", ID: core.NewMessageID(), Body: []byte("x"), DeliverAtMs: due}
		raw, err := core.EncodeMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Set(store.DelayKey(due, uint64(i)), raw); err != nil {
			t.Fatal(err)
		}
		es = append(es, entry{id: m.ID, due: due, seq: uint64(i)})
	}
	if err := f.st.Apply(b); err != nil {
		t.Fatal(err)
	}

	// 调度器持续跑
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := f.sc.Pass(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("Pass: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 并发撤回
	recalled := make([]bool, n)
	var rwg sync.WaitGroup
	for i := range es {
		rwg.Add(1)
		go func(i int) {
			defer rwg.Done()
			if _, err := f.sc.Recall(context.Background(), "t-race", es[i].due, es[i].seq); err == nil {
				recalled[i] = true
			}
		}(i)
	}
	rwg.Wait()

	// 让调度器把剩下的都搬完。不用定长 sleep 而是轮询：搬运每条约 2 次
	// fsync（第一段 + 第二段），全量耗时随机器磁盘速度差一个量级，定长
	// sleep 在慢机上要么不够（误报"丢了"）要么空等。轮询直到所有未被
	// 撤回的条目都已出现在 msg/（= 每条都有归宿），再断言不变式。
	deadline := time.Now().Add(10 * time.Second)
	for {
		delivered := deliveredIDs(t, f, "t-race")
		done := true
		for i, e := range es {
			if !recalled[i] {
				if _, ok := delivered[e.id]; !ok {
					done = false
					break
				}
			}
		}
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	delivered := deliveredIDs(t, f, "t-race")
	for i, e := range es {
		_, got := delivered[e.id]
		if recalled[i] && got {
			t.Fatalf("假成功：消息 %s 撤回返回成功，却仍被投递（seq=%d）", e.id, e.seq)
		}
		if !recalled[i] && !got {
			t.Fatalf("消息 %s 既没撤回成功也没被投递——丢了（seq=%d）", e.id, e.seq)
		}
	}
	t.Logf("并发不变式通过：撤回成功 %d 条、投递 %d 条，无交集无丢失",
		countTrue(recalled), len(delivered))
}
