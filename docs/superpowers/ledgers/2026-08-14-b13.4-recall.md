# Ledger: B13.4 RecallMessage (handoff 0396a2b5)

Branch: feat/recall-message
Baseline: 138db36 (== feat/auto-renew)

## Execution status
- [x] Task 1: AppendDelay 暴露 seq 与 staged
- [x] Task 2: recall 句柄编解码（域分隔）
- [x] Task 3: 调度器三道闸门与 Scheduler.Recall
- [x] Task 4: 发送侧签发句柄
- [x] Task 5: RecallMessage handler 与装配
- [x] Task 6: e2e（仅单机档）
- [x] Task 7: 全量验收与越界检查 + 终审

## Progress log
## 2026-08-14
- [x] Task1 done, review PASS (双裁决). commit 334da8a. Minor: produce_test.go:473 注释未直接断言 seq==0（无风险）; produce.go:261 导出注释重构. — 留终审 triage
- [x] Task2 done, review PASS. commit 4a86f25. Minor: U2b 属 sanity check 非判别用例（设计使然）; recall.go 边界注释前瞻引用 Task3 — 终审确认即可
- [x] Task3 done, review PASS (含偏离 A/B 独立验证：逐条 AppendDelay 单机档 i=14 staged=false 确实挂，批次必要; 轮询未弱化断言). commit 19de7ce.
  - Minor[留终审]: recall_test.go:108-109 注释高估 U7 单机档对缺重读的判别力（实验证明删重读 U7 仍绿，µs窗口 vs 1ms 扫描），建议收敛措辞，避免误导后人删闸门二
  - Minor: U7 批次 setup 自分配 seq 不读 delayalloc 计数器，不覆盖分配器路径（由 produce 测试兜底，可接受）
- [x] Task4 done, review PASS. commit 6c01c06. Minor: ledger-recall.md 未跟踪（终审决定去留）; env 复用风格提示（可忽略）
- [x] Task5 done, review PASS. commit e28913a. Minor: ErrRecallTopicMismatch rpc 侧不打日志（delay 侧已 Warn，可接受）; 两条 BAD_REQUEST 文案措辞不同指向同错误（琐碎）
- [x] Task6 done, review PASS. commit c86d466. Minor: E2 _= consumer.Ack 忽略错误（顺手 ack 非目标消息，可接受）; E2 超时/内容错共用 Fatal（诊断性略弱，非必修）
- [x] Task7 verification all green: gofmt(改动文件0)/build/vet/full internal tests/race(3 pkg)/U7 x20 race/e2e Recall|Delay 全绿
- [x] 终审 APPROVE_WITH_FIXES, 唯一必修 minor#3 (U7 注释诚实表述), 修复 commit 0021294, 范围复审 ACCEPT
- [x] 其余 minor(1,2,4-9) 终审裁决均「可接受」维持原样
