// snapstream.go 提供快照字节流的组键族枚举与分块编解码。
//
// 职责：
//   - groupKeyRanges：组 0 的全局前缀键族集合；数据组的五段前缀扫描区间
//   - scanGroupKeys：从只读视图枚举某组的全部键，逐键判哈希归属，
//     按 budget 分块、from 游标跨块续扫
//   - encodeChunk/decodeChunk：块的线格式 [4B 键长][键][4B 值长][值]
//
// 为什么数据组不能按前缀整段搬：队列→组是 fnv1a(topic, queueID) 哈希
// 取模的散布归属（见 groupForQueue），同一前缀（如 msg/T/）下的键散布
// 在多个数据组里——按前缀整段搬运会把别组的键搬进本组快照，接收方
// 落盘后键归属与本组组号错位，就是跨组污染。因此数据组枚举必须
// 逐键解析出 (topic, queueID) 再判归属，只有归属键才进入快照。
//
// 边界：
//   - 不含 metric/（本地不复制键族）与 raft/（日志区，不是 FSM）
//   - 不做字节流的传输与接收方落盘——那是 Task 6 控制通道的事
package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"

	"github.com/xushixin/sq/internal/store"
)

// 数据组五段键族前缀，与 store/keys.go 的 schema 同值（唯一事实源）。
// store 未导出 msg/ 等前缀常量，这里复刻一份；变更 schema 必须同步。
const (
	msgPrefix      = "msg/"
	allocPrefix    = "alloc/"
	keyIdxPrefix   = "keyidx/"
	cursorPrefix   = "cursor/"
	inflightPrefix = "inflight/"
)

// snapStreamLog 是快照枚举的包级日志出口。scanGroupKeys 的签名由计划
// 锁定（无 logger 参数），解析失败中止的 Error 只能走包级 logger；
// 生产装配把 handler 装在 slog.Default() 上（cmd/sq/main.go）。
var snapStreamLog = slog.Default().With("mod", "cluster.snapstream")

// keyRange 是快照枚举的一段键区间 [lower, upper)：下界闭、上界开，
// 与 store.Scan 的区间语义一致。
type keyRange struct {
	lower []byte
	upper []byte
}

// keyRanges 实现 sort.Interface：按 lower 字节序升序，供 groupKeyRanges
// 排序用（续扫单调性的前提之一，见 groupKeyRanges）。
type keyRanges []keyRange

func (r keyRanges) Len() int           { return len(r) }
func (r keyRanges) Less(i, j int) bool { return bytes.Compare(r[i].lower, r[j].lower) < 0 }
func (r keyRanges) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

// groupKeyRanges 返回组 g 的键扫描区间集。
//
// 组 0 是全局键族：meta/、delay/、half/、halfidx/ 连续前缀 + delayalloc
// 单键（延时 seq 计数器，无队列归属，只能整键搬）。**不含** metric/
// （本地不复制键族，进快照会让接收方把本节点私有采样点复制过去）
// 与 raft/（日志区，不是 FSM，接收方自己落盘日志）。
//
// 数据组（g>0）是五段前缀区间，归属过滤由 scanGroupKeys 逐键完成
// （见文件头注释：哈希散布，不能整段搬）。区间统一按字节序升序排列：
// 组 0 的全局前缀字面序即字节序（排序是恒等变换）；数据组的五段前缀
// 则必须重排为 alloc/ cursor/ inflight/ keyidx/ msg/——字面序 msg/
// 在首，若按字面序扫描，from 游标跨段后跳回字节序更小的键族，续扫
// 推进不单调：要么整族静默漏掉，要么整族反复重扫、永无 done。
//
// 升序由这里的 sort.Sort 无条件保证（键族表怎么写都不影响结果），
// 因此紧随其后的断言校验的是排序无法自动满足的那一半——两两不相交，
// 见 assertRangesDisjoint。两条合起来才是 scanGroupKeys 游标推进规则的
// 完整前提：升序保证「本段扫完才进下一段」，不相交保证「一个键只属于
// 一段」。
func groupKeyRanges(g uint32) []keyRange {
	meta := [][]byte{[]byte("delay/"), []byte("delayalloc"), []byte("half/"), []byte("halfidx/"), []byte("meta/")}
	if g == MetaGroup {
		ranges := make([]keyRange, 0, len(meta))
		for _, lower := range meta {
			ranges = append(ranges, keyRange{lower: lower, upper: store.PrefixUpperBound(lower)})
		}
		sort.Sort(keyRanges(ranges))
		assertRangesDisjoint(g, ranges)
		return ranges
	}
	data := [][]byte{[]byte(msgPrefix), []byte(allocPrefix), []byte(keyIdxPrefix), []byte(cursorPrefix), []byte(inflightPrefix)}
	ranges := make([]keyRange, 0, len(data))
	for _, lower := range data {
		ranges = append(ranges, keyRange{lower: lower, upper: store.PrefixUpperBound(lower)})
	}
	sort.Sort(keyRanges(ranges))
	assertRangesDisjoint(g, ranges)
	return ranges
}

// assertRangesDisjoint 断言排序后的区间集两两不相交且各自非空
// （lower < upper、前一段 upper ≤ 后一段 lower），违反即 panic。
//
// 为什么断言的是「不相交」而不是「已升序」：升序由紧邻上游的 sort.Sort
// 保证，排序之后再断言 IsSorted 恒真——那是一条永不触发的死代码，读起来
// 像不变量守卫，实际什么都没守。不相交才是排序无法自动满足、且真会被
// 未来的键族前缀改动破坏的性质：一旦有人加了 "msg" 与 "msg/" 这类互为
// 前缀的键族，两段区间重叠，scanGroupKeys 会把重叠部分发两遍（同一键
// 出现在两个块里），接收方按最后一次写入落盘——看似"能跑"，实则块与块
// 之间不再互斥，游标语义崩塌。
//
// 用 panic 而不是 error：这是装配期的代码错误（键族表写错），不是运行期
// 的对端/磁盘故障；静默漏发或重发比拒绝生成快照严重得多。
func assertRangesDisjoint(g uint32, ranges []keyRange) {
	bad := ""
	for i, r := range ranges {
		if bytes.Compare(r.lower, r.upper) >= 0 {
			bad = fmt.Sprintf("第 %d 段区间为空（lower %q ≥ upper %q）", i, r.lower, r.upper)
			break
		}
		if i > 0 && bytes.Compare(ranges[i-1].upper, r.lower) > 0 {
			bad = fmt.Sprintf("第 %d 段与第 %d 段重叠（前段 upper %q > 本段 lower %q）",
				i-1, i, ranges[i-1].upper, r.lower)
			break
		}
	}
	if bad == "" {
		return
	}
	lowers := make([]string, 0, len(ranges))
	for _, r := range ranges {
		lowers = append(lowers, fmt.Sprintf("%q", r.lower))
	}
	panic(fmt.Sprintf("cluster: 组 %d 键区间不满足两两不相交: %s；全部下界 [%s]",
		g, bad, strings.Join(lowers, " ")))
}

// groupForQueue 计算 topic+queueID 归属的数据组号（1..groups）。
//
// 入盘契约，永不可变——变更即存量数据错组，黄金值测试锁死。
// 算法：fnv1a(topic 字节 + 4B 大端 queueID) 对数据组数取模后偏移到
// [1, groups]；MetaGroup（0）不参与映射。Manager.GroupForQueue
// 是它的薄包装（注入 Manager 的数据组数）。
func groupForQueue(topic string, queueID uint32, groups uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], queueID)
	_, _ = h.Write(buf[:])
	return 1 + h.Sum32()%groups
}

// parseAllocKey 解析 alloc key：alloc/{topic}/{queueID:4B}。
// store 包尚无 ParseAllocKey（此前无调用方），这里按 ParseMsgKey 的
// 结构镜像：前缀后第一个 '/' 之前为 topic，其后必须恰好 4 字节
// BE queueID（二进制段可能含 '/'，只能按位置解析，不能 Split）。
func parseAllocKey(k []byte) (string, uint32, error) {
	rest, ok := bytes.CutPrefix(k, []byte(allocPrefix))
	if !ok {
		return "", 0, fmt.Errorf("非法 alloc key: %q", k)
	}
	i := bytes.IndexByte(rest, '/')
	if i < 0 || len(rest)-i-1 != 4 {
		return "", 0, fmt.Errorf("alloc key 结构错误: %q", k)
	}
	return string(rest[:i]), binary.BigEndian.Uint32(rest[i+1:]), nil
}

// keyGroupOf 解析键的 (topic, queueID) 并返回哈希归属组（1..groups）。
// 按键族前缀分派各自的 Parse 函数；解析失败返回错误——快照枚举必须
// 中止而不是跳过（漏键 = 静默丢数据，比拒绝生成快照严重得多）。
func keyGroupOf(k []byte, groups uint32) (uint32, error) {
	var topic string
	var qid uint32
	var err error
	switch {
	case bytes.HasPrefix(k, []byte(msgPrefix)):
		topic, qid, _, err = store.ParseMsgKey(k)
	case bytes.HasPrefix(k, []byte(allocPrefix)):
		topic, qid, err = parseAllocKey(k)
	case bytes.HasPrefix(k, []byte(keyIdxPrefix)):
		topic, _, _, qid, _, err = store.ParseKeyIdxKey(k)
	case bytes.HasPrefix(k, []byte(cursorPrefix)):
		topic, qid, err = store.ParseCursorTopicQueue(k)
	case bytes.HasPrefix(k, []byte(inflightPrefix)):
		_, topic, qid, _, err = store.ParseInflightKey(k)
	default:
		return 0, fmt.Errorf("键 %q 不属于任何数据键族", k)
	}
	if err != nil {
		return 0, err
	}
	return groupForQueue(topic, qid, groups), nil
}

// keyHexPrefix 取键前 32 字节的十六进制表示，供错误日志定位坏键——
// 键可能含不可打印二进制，%q 整键打出来刷屏且不可读。
func keyHexPrefix(k []byte) string {
	if len(k) > 32 {
		k = k[:32]
	}
	return hex.EncodeToString(k)
}

// scanGroupKeys 枚举组 g 的全部键：数据组逐键解析 (topic, queueID)
// 判哈希归属，归属 == g 的才交给 emit；跨块调用由 from 游标续扫。
//
// 参数：
//   - view: 钉住快照位点的只读视图
//   - g: 要枚举的组号（0 = 全局键族整段搬；>=1 数据组逐键过滤）
//   - groups: 数据组总数（哈希取模分母）
//   - from: 上一块发出的最后一个键（首块传 nil）；本块从它之后续扫，
//     保证块与块之间不重、不漏
//   - budget: 本块键数上限（必须 > 0）
//   - emit: 每命中一个归属键回调一次；返回错误立即中止整个枚举
//
// 返回：
//   - next: 本块最后发出的键（done=false 时作为下一块的 from）
//   - done: 全部范围是否已扫完（true 后调用方不得再续扫）
//   - err: 扫描错误或键解析错误——解析失败中止是刻意的：快照漏键 =
//     静默丢数据，比拒绝生成快照严重得多
//
// 注意：
//   - 枚举本身不打日志（每块几千键，打了就是刷屏）；每块的汇总由
//     调用方（Task 6 控制通道）打
//   - emit 收到的 k/v 底层内存仅回调期间有效，需要持有请自行拷贝
func scanGroupKeys(view *store.ReadView, g uint32, groups uint32, from []byte, budget int, emit func(k, v []byte) error) ([]byte, bool, error) {
	if budget <= 0 {
		return nil, false, fmt.Errorf("cluster: scanGroupKeys 键数预算 %d 必须为正", budget)
	}
	var last []byte // 本块最后发出的键 = 下一块的续扫游标
	n := 0
	skipped := false // from 键是否已被越过（from 上一块已发出，本块不得重发）
	stop := func(k, v []byte) (bool, error) {
		// Scan 下界是闭区间，从 from 续扫时先跳过 from 键本身
		if from != nil && !skipped {
			if bytes.Equal(k, from) {
				skipped = true
				return true, nil
			}
		}
		skipped = true
		// 数据组逐键判归属：归属 != g 的键静默跳过，不占 budget
		if g != MetaGroup {
			kg, perr := keyGroupOf(k, groups)
			if perr != nil {
				snapStreamLog.Error("快照枚举中止：键解析失败", "g", g, "key_hex", keyHexPrefix(k), "err", perr)
				return false, fmt.Errorf("cluster: 快照枚举组 %d 解析键 0x%s 失败: %w", g, keyHexPrefix(k), perr)
			}
			if kg != g {
				return true, nil
			}
		}
		last = append(last[:0], k...)
		if err := emit(k, v); err != nil {
			return false, err
		}
		n++
		return n < budget, nil
	}
	for _, r := range groupKeyRanges(g) {
		lower := r.lower
		// 游标推进规则（区间按字节序升序，见 groupKeyRanges）：
		//   - 整个范围在游标之前（upper <= from）：上一块已扫完，整段跳过，
		//     否则会重发已发出的键——每块重发 = 永无 done
		//   - 游标落在本范围内：从游标续扫（stop 负责跳过游标键本身）
		//   - 游标在本范围之后（lower > from）：本范围尚未扫到，整段从下界扫
		if from != nil && !skipped {
			if bytes.Compare(from, r.upper) >= 0 {
				continue
			}
			if bytes.Compare(from, r.lower) >= 0 {
				lower = from
			}
		}
		if err := view.Scan(lower, r.upper, 0, stop); err != nil {
			return last, false, err
		}
		if n >= budget {
			return last, false, nil
		}
	}
	return last, true, nil
}

// kv 是快照块里的一对键值。值是 FSM 原样字节（接收方 Store.Apply
// 直接可写），不做任何序列化包装。
type kv struct {
	k []byte
	v []byte
}

// encodeChunk 把一组键值对编码为块的线格式：
// [4B 键长][键][4B 值长][值] 重复，长度全部大端。
func encodeChunk(pairs []kv) []byte {
	buf := make([]byte, 0, 8*len(pairs))
	for _, p := range pairs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.k)))
		buf = append(buf, p.k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.v)))
		buf = append(buf, p.v...)
	}
	return buf
}

// decodeChunk 解码块的线格式；逐段校验剩余长度，坏块报错。
// 对端或盘上数据损坏时静默截断 = 静默丢数据，必须拒收报错。
func decodeChunk(b []byte) ([]kv, error) {
	var out []kv
	for len(b) > 0 {
		if len(b) < 4 {
			return nil, fmt.Errorf("cluster: 快照块剩余 %d 字节不足键长度头", len(b))
		}
		kl := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		if uint32(len(b)) < kl {
			return nil, fmt.Errorf("cluster: 快照块键长 %d 超出剩余 %d 字节", kl, len(b))
		}
		k := b[:kl]
		b = b[kl:]
		if len(b) < 4 {
			return nil, fmt.Errorf("cluster: 快照块剩余 %d 字节不足值长度头", len(b))
		}
		vl := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		if uint32(len(b)) < vl {
			return nil, fmt.Errorf("cluster: 快照块值长 %d 超出剩余 %d 字节", vl, len(b))
		}
		v := b[:vl]
		b = b[vl:]
		out = append(out, kv{k: k, v: v})
	}
	return out, nil
}
