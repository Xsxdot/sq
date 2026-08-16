# 分支合并收口（2026-08-16）

> 把长期挂在本地、互相叠着的一批功能分支一次性并进 `main`。
> 记录**选了什么合并次序、为什么、以及哪些分支刻意不合**。
> 合并后 main tip：`0a23542`。**未推 origin**。

## 结论先行

**合并次序 = 一次合并。** 不逐条把功能分支重放到 main，而是把已经整体验收过的
集成基线 `verify/final-integration-3` 快进到 main。

判据只有一条：**被验收的是那棵树，不是那些分支。**

全量回归（18 包单测、Go e2e 48 PASS/0 FAIL/2 SKIP、Python 深水区 12/0、
Java 深水区 10/0、Linux 真机 -race、跨物理机三节点集群）跑的都是集成基线这一棵树。
逐条重放会得到九个**从未被测过的中间态**，而且每条分支各自的冲突解法未必能拼回
同一棵树——那样交付的将是一棵没人验过的树，附带一份验的是别的树的报告。

## 实际动作

```
verify/final-integration-3 (93d152e)   ← 已验收的树
        + merge main (7589060，纯 docs)  → 0a23542
main    --ff-only--> 0a23542
```

两处核验，缺一不可：

1. `git diff --stat 93d152e 0a23542 -- . ':(exclude)docs'` **输出为空**
   ——合入 main 的那个 docs 提交没有碰任何代码，合并后的代码树与已验收的树
   逐字节相同。
2. 合并后的 main 上 `go build ./... && go test ./...`：**18 包 ok，0 FAIL**。

`--ff-only` 是刻意的：它保证 main 不会引入任何「只在 main 上存在」的新树。
如果这一步不是快进（说明 main 上还有集成基线没吃进去的东西），应当停下来
先把它并进集成基线再验，而不是在 main 上就地解冲突。

## 合入了什么

十二条分支（八条功能分支 + 四条派发用的 base 基线）现均为 main 的祖先：

| 条目 | 分支 | tip | 进入路径 |
|---|---|---|---|
| B13.1 SQL92 过滤 | `feat/sql92-filter` | `e5ca117` | 栈底 |
| B13.2 PushConsumer e2e | `base/b13.2-on-sql92` → `feat/push-consumer-e2e` | `b364deb` | 叠在 B13.1 上 |
| B13.5 AutoRenew | `base/b13.5-on-b13.2` → `feat/auto-renew` | `138db36` | 叠在 B13.2 上 |
| B13.4 RecallMessage | `feat/recall-message` | `8688427` | merge `9ae7d61` |
| B13.6 按客户端类型退避 | `feat/retry-policy-per-client` | `265631e` | merge `d5164a5` |
| B12 可诊断性 | `base/b12-on-main` → `fix/b12-diagnosability-2` | `f2de32b` | merge `629151c` |
| B14/B15 观察者竞态 + 日志级别 | `base/b14-b15-on-main` → `fix/b14-b15-observer-and-loglevel` | `dbb2d03` | merge `6617bbf` |
| B13.8 信封层 delivery_timestamp | `fix/b13.8-receive-delivery-timestamp` | `ab0cce9` | merge `93d152e` |

前三条是**线性叠栈**（派发时后一条就以前一条为基线），不是合并——这是它们在
派发阶段就定下的依赖次序，本次没有重新排。

## 刻意不合的三条

| 分支 | 相对集成基线独有 | 不合的理由 |
|---|---|---|
| `m2-retry-dlq-tag-keys-retention` | 1 个 docs 提交 | 该文件在集成基线里已存在且**逐字节相同**，是同内容不同 commit hash。无内容可合。 |
| `v2-b2-seglog` | 3 份 spec/plan | 集成基线的版本**严格更新**：多出 §7 三机实测结果表、容量下界订正（元数据组也预分配）、`-race` 18 包留痕。合过去等于用旧版覆盖新版。 |
| `v2-sharedlog-gc` | 5 个真实 feature 提交 | **验收失败的实验**：共享 seglog 跨组 group commit 在 fsync 档稳定劣于每组独立 15-17%。方向已被 `feat/seglog-prealloc-fdatasync`（已合）取代。 |

前两条是「看着有东西、实际没有」，第三条是「有东西但不该要」——三条都不该按
「未合入就合一下」的惯性处理。分支保留不删，作为负结果的取证。

`Xsxdot/grok-sq-m1`、`Xsxdot/v4flash-sq-m1`、`Xsxdot/real-test` 是其他会话仍在
使用的 worktree 分支，不属本次收口范围。

## 未做

- **未推 `origin/main`**。本地 main 现领先 origin/main **104 个提交**
  （first-parent 36 个），且这次不再只是 docs——64 个非 docs 文件有差异。
  推送是单独的决定。**在推之前，远程 handoff 派发不能再用 `--base main`**：
  基线校验只查 commit 存在性，executor 会拿到 origin 上那份缺 v1.1 全部改动的旧代码。
- **C# 深水区未针对 B13.8 回归**（环境在已失联的阿里云机上，联想机无 .NET）。
  该缺口在 [多语言 SDK 深水区验证](2026-08-15-multilang-sdk-deep-verification.md)
  里已单独记明，不因合并而消失。
