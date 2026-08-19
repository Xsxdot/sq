// 取件租约：自动续租不可见期的参数与判据。
//
// 职责：
//   - 描述一次取件的续租参数（持有者是谁、预算多长、如何判定存活）
//   - 给出「一条已到期的 inflight 该续租还是该重投」的唯一判据
//
// 边界：
//   - 不做存活判定本身——判定逻辑属于 rpc 层的会话注册表，这里只收一个闭包。
//     这样 deliver 不反向依赖 rpc，也避免了「构造后 setter 忘了调就静默不续租」
//     的哑火面
//   - 不碰 Attempts / DLQ / 退避：续租只影响「何时重投」，不影响「重投时怎么算」
package deliver

import (
	"time"

	"github.com/Xsxdot/sq/internal/core"
)

// Lease 本次取件的租约参数。
//
// 三个字段任一为零值即视为不启用续租（见 Enabled）——调用方漏配任何一项都
// 整体退化成固定不可见期的既有行为，不会出现「半开」的中间态。
type Lease struct {
	// Owner 本次取件方的客户端标识（x-mq-client-id）。本轮投出去的消息
	// 会把它记进 InflightState.Owner 作为归属。
	Owner string
	// MaxRenew 单次投递允许续租的总时长上限。投递时换算成绝对时刻记进
	// InflightState.RenewUntilMs。
	MaxRenew time.Duration
	// Alive 判定某个持有者是否仍有活跃的 Telemetry 会话。
	// 注意入参是**记录里的持有者**，不一定等于本 Lease 的 Owner——重平衡后
	// 轮询方与持有者会是两个不同的客户端。
	Alive func(string) bool
}

// Enabled 本次取件是否启用自动续租。
func (l Lease) Enabled() bool {
	return l.Owner != "" && l.MaxRenew > 0 && l.Alive != nil
}

// receiveOpts 取件可选参数的聚合。
type receiveOpts struct {
	lease Lease
}

// ReceiveOption 取件可选参数。
//
// 之所以用变参而不是给 Receive 加形参：Receive 只有 1 处生产调用点，却有
// 约 90 处测试调用点，改签名的清扫成本极高（见 backlog B13.1「接口化地雷」：
// 上一次改它造成了编译期无感、运行期 panic 的大面积清扫）。变参尾置让既有
// 调用点一字不改照常编译。
type ReceiveOption func(*receiveOpts)

// WithLease 为本次取件启用自动续租。
//
// 传入的 Lease 不满足 Enabled() 时等价于不传——调用方不需要自己做条件判断。
func WithLease(l Lease) ReceiveOption {
	return func(o *receiveOpts) { o.lease = l }
}

// renewable 判定一条**已到期**的 inflight 应当续租而非重投。
//
// 调用前提：调用方已确认 st.ExpireAtMs <= nowMs。四条判据全真才续租，任一
// 不成立都回落到原重投路径——失败方向永远是「重投」，因为重投是 at-least-once
// 契约内的既有行为，而错误地续租会让消息被一个可能已经死掉的持有者扣住，
// 直到硬上限才释放。
func renewable(l Lease, st *core.InflightState, nowMs int64) bool {
	// 判据 1 必须先行：Enabled() 保证了 l.Alive 非 nil，判据 4 才能直接调用
	if !l.Enabled() {
		return false
	}
	// 判据 2：0 表示旧格式记录或本就不续租；<= nowMs 表示续租预算已耗尽
	if st.RenewUntilMs <= nowMs {
		return false
	}
	// 判据 3：归属未知就无从判定存活，保守重投
	if st.Owner == "" {
		return false
	}
	// 判据 4：问的是记录里的持有者，不是本次轮询方
	return l.Alive(st.Owner)
}
