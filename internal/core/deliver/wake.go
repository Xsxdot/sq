// wake.go 长轮询唤醒表（卡 SQ4.2）。
//
// 职责：
//   - 等待者按 (消费组, topic, 队列) 登记一次性唤醒信号（subscribe）
//   - 新消息在 (topic, 队列) 上可见：唤醒该队列下全部消费组（wakeQueue，
//     由 produce 的可见性监听回调触发）
//   - 本组在该队列上的消费状态变化成功（Ack 一类）：只唤醒同一
//     (组, topic, 队列)（wakeGroupQueue，计划 P3）
//
// 边界：
//   - 只管信号，不判可见性、不读写消费状态；不挂在 queueState 上（计划 P1：
//     produce 回调若要取队列锁，会在 moveToDLQ 持源队列锁写死信时形成两把
//     队列锁嵌套）
//   - 锁序：w.mu 是叶子锁——持它时不取任何别的锁、不做 I/O。调用方持有
//     其他锁（含队列锁）时调用本文件函数不会成环
//   - 信号是 close 广播：被唤醒者须重新 subscribe。订阅了但始终没被唤醒的
//     条目（等待者超时退出）留到下次唤醒才清，规模上限 = 曾被长轮询过的
//     (组, topic, 队列) 数，与 Deliverer.qmu 同量级
package deliver

import "sync"

// wakeQKey 唤醒表一级索引：(topic, 队列)。
type wakeQKey struct {
	topic string
	q     uint32
}

// wakeTable (topic, 队列) → 消费组 → 一次性信号。零值不可用，用 newWakeTable。
type wakeTable struct {
	mu sync.Mutex
	m  map[wakeQKey]map[string]chan struct{}
}

func newWakeTable() *wakeTable {
	return &wakeTable{m: map[wakeQKey]map[string]chan struct{}{}}
}

// subscribe 返回 (group, topic, q) 的唤醒信号：该队列下次有新消息可见，或本组
// 在该队列上的状态变化成功时被 close。同一 (组, 队列) 的多个等待者共享同一信号。
func (w *wakeTable) subscribe(group, topic string, q uint32) <-chan struct{} {
	k := wakeQKey{topic, q}
	w.mu.Lock()
	defer w.mu.Unlock()
	gs, ok := w.m[k]
	if !ok {
		gs = map[string]chan struct{}{}
		w.m[k] = gs
	}
	ch, ok := gs[group]
	if !ok {
		ch = make(chan struct{})
		gs[group] = ch
	}
	return ch
}

// wakeQueue 唤醒 (topic, q) 下全部消费组的等待者并撤下信号。
func (w *wakeTable) wakeQueue(topic string, q uint32) {
	k := wakeQKey{topic, q}
	w.mu.Lock()
	gs := w.m[k]
	delete(w.m, k)
	w.mu.Unlock()
	for _, ch := range gs {
		close(ch)
	}
}

// wakeGroupQueue 只唤醒 (group, topic, q) 的等待者并撤下其信号。
func (w *wakeTable) wakeGroupQueue(group, topic string, q uint32) {
	k := wakeQKey{topic, q}
	w.mu.Lock()
	var ch chan struct{}
	if gs, ok := w.m[k]; ok {
		ch = gs[group]
		delete(gs, group)
		if len(gs) == 0 {
			delete(w.m, k)
		}
	}
	w.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}
