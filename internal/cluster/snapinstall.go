// snapinstall.go 提供快照接收侧安装：拉块线格式与清空重来。
//
// 职责：
//   - wipeGroupKeys：删除一个组在共享 store 里的全部 FSM 键——
//     组 0 按连续前缀 DeleteRange 整段删；数据组逐键解析判哈希归属后
//     只删本组键
//   - encodeSnapFetchReq/decodeSnapFetchResp：OpFetchSnapshot 拉块的
//     请求/响应线格式（生产侧编码；发送侧测试的 encodeFetchReq/
//     decodeFetchResp 是 cluster_test.go 里的对等测试助手，互不复用）
//
// 安装主流程（installSnapshot 六步与收口批次）在 group.go，本文件只
// 提供它用到的线格式与清空原语。
//
// 为什么先标记后清空（顺序即不变量，installSnapshot 第 2 步早于第 3 步）：
// 安装中标记（raft/<g>/snapinstall，raftstore.MarkInstalling Sync 落盘）
// 必须先于任何数据写入——反过来（先清空后标记）的崩溃窗口里，磁盘上
// 是「已清空、无标记」= 静默空状态，重启会把它当完整状态启动，客户端
// 读到的消息永久缺失。先标记后清空则崩溃只留下「有标记」的半截状态，
// 重启经 buildGroup 的标记检查清空重来。
//
// 边界：
//   - 不含 raft/ 日志区键（日志归 raft 层，wipe 不碰）与 metric/
//     （本地不复制键族，与快照枚举同纪律）
//   - 不做安装主流程（installSnapshot 六步与收口批次，Task 7 part B）；
//     applied/锚点/成员表由 raftStore.ResetGroupProgress 重置
//   - 键归属判定复用 snapstream.go 的 groupKeyRanges/keyGroupOf——
//     清空与枚举必须同一份解析与同一套归属规则，分叉即漏清/误清
package cluster

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/xushixin/sq/internal/store"
)

// snapInstallLog 是清空重来的包级日志出口。wipeGroupKeys 的签名由计划
// 锁定（无 logger 参数），解析失败中止的 Error 只能走包级 logger；
// 与 snapStreamLog 同模式（生产装配把 handler 装在 slog.Default() 上）。
var snapInstallLog = slog.Default().With("mod", "cluster.snapinstall")

// encodeSnapFetchReq 编码 OpFetchSnapshot 拉块请求（installSnapshot
// 第 4 步用）：[4B BE 组][8B BE snapID][4B BE 游标键长][游标键]，全部
// 大端。首块游标为 nil（长度 0）；后续块以上一块响应的「下一游标键」
// 续拉。与 manager.go handleFetchSnapshot 的解析侧唯一配对。
func encodeSnapFetchReq(g uint32, snapID uint64, cursor []byte) []byte {
	req := make([]byte, 16+len(cursor))
	binary.BigEndian.PutUint32(req[:4], g)
	binary.BigEndian.PutUint64(req[4:12], snapID)
	binary.BigEndian.PutUint32(req[12:16], uint32(len(cursor)))
	copy(req[16:], cursor)
	return req
}

// decodeSnapFetchResp 解码 OpFetchSnapshot 拉块响应：[1B 是否结束][4B BE
// 下一游标键长][下一游标键][块字节]。done=true 后不得再以 next 续拉。
//
// 坏布局必须报错而不是静默截断——截断一块 = 本组状态永久缺失一块，
// 与快照枚举/清空的「中止绝不跳过」同一纪律。
func decodeSnapFetchResp(resp []byte) (done bool, next, chunk []byte, err error) {
	if len(resp) < 5 {
		return false, nil, nil, fmt.Errorf("cluster: FetchSnapshot 响应 %d B 过短（不足 1B 结束位 + 4B 游标长）", len(resp))
	}
	done = resp[0] == 1
	nl := binary.BigEndian.Uint32(resp[1:5])
	if uint32(len(resp)-5) < nl {
		return false, nil, nil, fmt.Errorf("cluster: FetchSnapshot 响应游标键长 %d 超出剩余 %d B", nl, len(resp)-5)
	}
	next = resp[5 : 5+int(nl)]
	chunk = resp[5+int(nl):]
	return done, next, chunk, nil
}

// wipeGroupKeys 删除组 g 在共享 store 里的全部 FSM 键（清空重来）。
//
// 参数：
//   - st: 共享 store（raft 日志与 FSM 数据同库，本函数只碰 FSM 键）
//   - g: 要清空的组号（0 = 全局键族；数据组 1..groups 逐键判归属）
//   - groups: 数据组总数（哈希取模分母，与 groupForQueue 同源）
//
// 返回：错误。扫描中的键解析失败即中止并返回错误——清空漏键 = 半截
// 状态残留，与快照枚举同一规则：中止，绝不跳过（跳过 = 静默丢数据）。
//
// 注意：
//   - 提交用 NoSync（st.Apply）：清空是「安装中标记已 Sync 落盘」的
//     后续动作，崩溃最多留下多余键——重启见标记即重新清空，重装也会
//     覆盖这些键，无需为清空本身付 fsync 代价。
//   - 组 0 是全局连续前缀（meta/、delay/、half/、halfidx/、delayalloc
//     单键），DeleteRange 整段删；数据组键哈希散布（groupForQueue），
//     整段删会把别组的数据一起清掉，必须逐键解析后只删本组键。
func wipeGroupKeys(st *store.Store, g, groups uint32) error {
	b := st.NewBatch()
	for _, r := range groupKeyRanges(g) {
		if g == MetaGroup {
			// 组 0：连续前缀整段删（delayalloc 单键也由自身前缀区间覆盖）
			if err := b.DeleteRange(r.lower, r.upper); err != nil {
				b.Close()
				return fmt.Errorf("cluster: 清空组 %d 删区间 [%q,%q): %w", g, r.lower, r.upper, err)
			}
			continue
		}
		// 数据组：逐键解析 (topic, queueID) 判哈希归属，只删本组键。
		// 同一扫描批次内 b.Delete 只记录在批里，不触碰迭代器。
		n := 0
		if err := st.Scan(r.lower, r.upper, 0, func(k, _ []byte) (bool, error) {
			kg, perr := keyGroupOf(k, groups)
			if perr != nil {
				snapInstallLog.Error("清空中止：键解析失败", "g", g, "key_hex", keyHexPrefix(k), "err", perr)
				return false, fmt.Errorf("cluster: 清空组 %d 解析键 0x%s 失败: %w", g, keyHexPrefix(k), perr)
			}
			if kg == g {
				if err := b.Delete(k); err != nil {
					return false, fmt.Errorf("cluster: 清空组 %d 删键 0x%s: %w", g, keyHexPrefix(k), err)
				}
				n++
			}
			return true, nil
		}); err != nil {
			b.Close()
			return err
		}
		snapInstallLog.Debug("键族已清空", "g", g, "prefix", string(r.lower), "keys", n)
	}
	if err := st.Apply(b); err != nil {
		return fmt.Errorf("cluster: 清空组 %d 提交: %w", g, err)
	}
	return nil
}
