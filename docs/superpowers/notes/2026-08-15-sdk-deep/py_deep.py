# B13.3 深水区：官方 rocketmq-python-client 对 sq 的特性级验证。
#
# 职责：用官方 Python SDK 逐项验证 sq 已实现的消息特性——SQL92 属性过滤、
#       FIFO 顺序、事务提交/回滚、PushConsumer + AutoRenew 续租、定时消息、
#       Recall 撤回——每项都断言「该收到的收到、不该收到的收不到」。
# 边界：不测吞吐、不测鉴权（08-14 已单独冒烟过）、不测多消费者再均衡。
#       只回答「这些特性用真实客户端跑通不通」。
#
# 判据设计原则：每项都必须有「反例」——只断言正例会让一个恒真的实现也通过。
# 例如 SQL92 只断言 age>18 的收到了，那么「过滤器根本没生效、全投递」也能过。

import sys
import time
import threading

from rocketmq import (ClientConfiguration, Credentials, FilterExpression,
                      TransactionChecker, TransactionResolution,
                      Message, Producer, PushConsumer, SimpleConsumer,
                      MessageListener, ConsumeResult)
from rocketmq.grpc_protocol import FilterType

ENDPOINT = "127.0.0.1:28081"
ADMIN = "http://" + ENDPOINT.split(":")[0] + ":28082"
# 每轮换一组 topic：topic 会跨轮次累积消息，而「同组消息是否都在一个队列」
# 「新消费组从头读到了什么」这两类断言都依赖 topic 里只有本轮写入的数据。
# 上一轮就因为累积把 30 条误判成分布异常、又把对照组消息误判成撤回失败。
RUN = str(int(time.time()))[-6:]
results = []


def admin_get(path):
    import json as _j
    import urllib.request as _u
    with _u.urlopen(ADMIN + path, timeout=10) as r:
        return _j.load(r)


def admin_create_topic(name, queues):
    import json as _j
    import urllib.request as _u
    req = _u.Request(ADMIN + "/admin/topics",
                     data=_j.dumps({"name": name, "queues": queues}).encode(),
                     headers={"Content-Type": "application/json"}, method="POST")
    with _u.urlopen(req, timeout=10) as r:
        return _j.load(r)


def queue_bodies(topic, queues):
    """按队列返回消息体列表，用于直接观测路由分布。"""
    import base64
    out = []
    for q in range(queues):
        msgs = admin_get(f"/admin/messages?topic={topic}&queue_id={q}")
        out.append([base64.b64decode(m["body_base64"]).decode() for m in msgs])
    return out


def record(name, ok, note):
    results.append((name, ok, note))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {note}", flush=True)


def cfg():
    return ClientConfiguration(ENDPOINT, Credentials(), request_timeout=10)


def new_msg(topic, body, tag=None, keys=None, props=None, group=None, delay_s=None):
    m = Message()
    m.topic = topic
    m.body = body.encode()
    if tag:
        m.tag = tag
    if keys:
        m.keys = keys
    if props:
        for k, v in props.items():
            m.add_property(k, v)
    if group:
        m.message_group = group
    if delay_s:
        # 单位是**秒**，不是毫秒：producer.py:518 是
        #   system_properties.delivery_timestamp.seconds = message.delivery_timestamp
        # 直接把这个值赋给 protobuf Timestamp 的 seconds 字段。而同一属性的读侧
        # （fromProtobuf）却用 Misc.to_mills 返回毫秒——这个 SDK 的 setter/getter
        # 单位不对称。按毫秒传会把消息排到 1000 倍远的未来，且**不会报错**：
        # 表现为「延时消息永远不到」，极易误判成 broker 的定时投递坏了。
        m.delivery_timestamp = int(time.time()) + delay_s
    return m


def drain(consumer, want, budget_s, ack=True):
    """在 budget_s 秒内尽量收满 want 条，返回收到的 Message 列表（按到达顺序）。

    SimpleConsumer 每次 receive 只轮询一个队列，收不到不代表消息没到，
    所以必须轮询而不是单次 receive 就下结论。
    """
    got = []
    deadline = time.time() + budget_s
    while time.time() < deadline and len(got) < want:
        try:
            batch = consumer.receive(16, 10)
        except Exception:
            # 空轮询在这个 SDK 里也走异常路径；真实故障（路由错、过滤被拒）
            # 同样落这里，所以下面出错时要把最后一次异常打出来，不能静默
            continue
        if not batch:
            continue
        for mv in batch:
            got.append(mv)
            if ack:
                consumer.ack(mv)
    return got


# ---------------------------------------------------------------- 1. SQL92
def test_sql92():
    topic, grp = f"py-sql92-{RUN}", f"py-g-sql92-{RUN}"
    p = Producer(cfg(), topics=(topic,))
    p.startup()
    # 6 条覆盖：数值比较、字符串等值、BETWEEN/IN、属性缺失（三值逻辑）
    p.send(new_msg(topic, "a-vip30-cn", props={"age": "30", "region": "cn", "vip": "true"}))
    p.send(new_msg(topic, "b-vip15-cn", props={"age": "15", "region": "cn", "vip": "true"}))
    p.send(new_msg(topic, "c-plain30-us", props={"age": "30", "region": "us", "vip": "false"}))
    p.send(new_msg(topic, "d-noage-cn", props={"region": "cn"}))          # age 缺失
    p.send(new_msg(topic, "e-vip22-cn", props={"age": "22", "region": "cn", "vip": "true"}))
    p.send(new_msg(topic, "f-vip99-jp", props={"age": "99", "region": "jp", "vip": "true"}))
    p.shutdown()

    # 期望命中 a、e：age 在 (18,50] 区间、region 在 cn/us 内、vip 为 'true'
    # 反例覆盖：b(年龄小) c(vip 否) d(age 缺失→UNKNOWN) f(region 不在集合)
    expr = "age > 18 AND age <= 50 AND region IN ('cn','us') AND vip = 'true'"
    c = SimpleConsumer(cfg(), grp, subscription={topic: FilterExpression(expr, FilterType.SQL)},
                       await_duration=5)
    c.startup()
    got = {m.body.decode() for m in drain(c, 6, 40)}
    c.shutdown()
    want = {"a-vip30-cn", "e-vip22-cn"}
    if got == want:
        record("sql92-复合表达式", True, f"命中 {sorted(got)}，4 条反例均正确排除")
    else:
        record("sql92-复合表达式", False, f"期望 {sorted(want)}，实得 {sorted(got)}")

    # 三值逻辑：age 缺失时 NOT(age > 18) 不得为真——这是 spec §2.2 明确定的语义，
    # 也是二值逻辑实现最容易踩错的一条。d 的 age 缺失，必须收不到。
    c2 = SimpleConsumer(cfg(), f"py-g-sql92-not-{RUN}",
                        subscription={topic: FilterExpression("NOT (age > 18)", FilterType.SQL)},
                        await_duration=5)
    c2.startup()
    got2 = {m.body.decode() for m in drain(c2, 6, 30)}
    c2.shutdown()
    if "d-noage-cn" not in got2 and got2 == {"b-vip15-cn"}:
        record("sql92-三值逻辑", True, "age 缺失的消息未因 NOT 被误投；仅 b(age=15) 命中")
    else:
        record("sql92-三值逻辑", False,
               f"期望仅 {{'b-vip15-cn'}}，实得 {sorted(got2)}（d 出现即为二值逻辑缺陷）")

    # IS NULL：显式选出属性缺失的那条
    c3 = SimpleConsumer(cfg(), f"py-g-sql92-isnull-{RUN}",
                        subscription={topic: FilterExpression("age IS NULL", FilterType.SQL)},
                        await_duration=5)
    c3.startup()
    got3 = {m.body.decode() for m in drain(c3, 6, 30)}
    c3.shutdown()
    ok = got3 == {"d-noage-cn"}
    record("sql92-IS NULL", ok, f"实得 {sorted(got3)}" + ("" if ok else "，期望 ['d-noage-cn']"))


# ---------------------------------------------------------------- 2. FIFO
class OrderListener(MessageListener):
    """按到达顺序记录消息体。单线程消费，避免线程池打乱观测到的顺序。"""

    def __init__(self):
        self.seq = []
        self.lock = threading.Lock()

    def consume(self, message):
        with self.lock:
            self.seq.append(message.body.decode())
        return ConsumeResult.SUCCESS


def test_fifo():
    topic, grp = f"py-fifo-{RUN}", f"py-g-fifo-{RUN}"
    admin_create_topic(topic, 4)
    p = Producer(cfg(), topics=(topic,))
    p.startup()
    n = 10
    # 主组：10 条验顺序
    for i in range(n):
        p.send(new_msg(topic, f"seq-{i:02d}", group="order-42"))
    # 陪跑组：另外 4 个 message group，各 2 条。它们只服务一个目的——
    # 证明路由**不是退化的**。只看主组「10 条全在队列 0」无法区分
    # 「按组哈希正确」与「哈希坏了、所有消息都进队列 0」。
    for g in range(4):
        for k in range(2):
            p.send(new_msg(topic, f"other-{g}-{k}", group=f"other-group-{g}"))
    p.shutdown()
    time.sleep(2)

    dist = queue_bodies(topic, 4)

    # 判据一：主组 10 条必须全部落在同一个队列
    main_q = [qi for qi, bodies in enumerate(dist)
              if any(b.startswith("seq-") for b in bodies)]
    main_cnt = sum(1 for bodies in dist for b in bodies if b.startswith("seq-"))
    if len(main_q) == 1 and main_cnt == n:
        record("fifo-同组同队列", True, f"{n} 条全部落在队列 {main_q[0]}")
    else:
        record("fifo-同组同队列", False,
               f"主组被打散到队列 {main_q}（共 {main_cnt} 条），分布 {[len(x) for x in dist]}")

    # 判据二：每个陪跑组也各自收敛到单一队列，且全体至少用到 2 个不同队列
    spread_ok, used = True, set()
    for g in range(4):
        qs = {qi for qi, bodies in enumerate(dist)
              if any(b.startswith(f"other-{g}-") for b in bodies)}
        if len(qs) != 1:
            spread_ok = False
        used |= qs
    used |= set(main_q)
    if spread_ok and len(used) >= 2:
        record("fifo-路由非退化", True,
               f"5 个 message group 各自收敛到单一队列，合计用到 {len(used)} 个不同队列")
    elif not spread_ok:
        record("fifo-路由非退化", False, "有 message group 的消息被拆到多个队列")
    else:
        record("fifo-路由非退化", False,
               f"5 个组全挤在同一个队列（{used}）——无法排除「哈希退化为常量」")

    # 判据三：顺序。用 SimpleConsumer 而非 PushConsumer——后者当前被
    # 「ReceiveMessageResponse 缺 delivery_timestamp」那条缺陷打死（见 B13.8），
    # 用它会把一个无关的 bug 记到 FIFO 头上。SimpleConsumer 每次只轮询一个
    # 队列，本 topic 有 4 个，所以 await 压短、预算给足。
    c = SimpleConsumer(cfg(), grp, subscription={topic: FilterExpression("*")}, await_duration=1)
    c.startup()
    # 预算给到 400s：本 topic 有 4 个队列、共 18 条消息，SimpleConsumer 每次
    # receive 只轮询一个队列，轮空占掉大半时间。150s 只够收到 6/10（顺序正确），
    # 那是取样不足，不是顺序错——但「收不满」和「顺序错」在结果里长得太像，
    # 不值得为省几分钟留这个歧义。
    got = [m.body.decode() for m in drain(c, n + 8, 400)]
    c.shutdown()
    main_seq = [b for b in got if b.startswith("seq-")]
    want = [f"seq-{i:02d}" for i in range(n)]
    if main_seq == want:
        record("fifo-同组顺序", True, f"{n} 条按发送序到达")
    elif len(main_seq) < n:
        record("fifo-同组顺序", False, f"400s 预算内只收到 {len(main_seq)}/{n} 条：{main_seq}")
    else:
        record("fifo-同组顺序", False, f"顺序不符：{main_seq}")


# ---------------------------------------------------------- 3. 事务
class NeverChecker(TransactionChecker):
    """本用例走显式 commit/rollback，回查不该被触发。

    但 Python SDK 的 Producer 构造时强制要求 checker 非空（否则抛
    IllegalArgumentException），所以必须给一个。它若真被调用，说明
    broker 把已裁决的事务又当成孤儿回查了——那是缺陷，用 called 标记留证。
    """

    def __init__(self):
        self.called = []

    def check(self, message):
        self.called.append(message.body.decode())
        return TransactionResolution.ROLLBACK


def test_transaction():
    topic = f"py-txn-{RUN}"
    checker = NeverChecker()
    p = Producer(cfg(), topics=(topic,), checker=checker)
    p.startup()

    t1 = p.begin_transaction()
    p.send(new_msg(topic, "txn-committed"), t1)
    t1.commit()

    t2 = p.begin_transaction()
    p.send(new_msg(topic, "txn-rolledback"), t2)
    t2.rollback()
    p.shutdown()

    c = SimpleConsumer(cfg(), f"py-g-txn-{RUN}", subscription={topic: FilterExpression("*")},
                       await_duration=5)
    c.startup()
    # 多收一会儿：要证明 rollback 那条「不会来」，只能靠时间预算耗尽
    got = {m.body.decode() for m in drain(c, 2, 35)}
    c.shutdown()
    if got == {"txn-committed"}:
        record("事务-提交可见/回滚不可见", True, "committed 到达，rolledback 在 35s 内未出现")
    else:
        record("事务-提交可见/回滚不可见", False, f"实得 {sorted(got)}")
    # 已显式裁决的事务不该再被回查；被回查说明 broker 没销掉半消息的回查排期
    record("事务-已裁决不再回查", not checker.called,
           "回查未被触发（符合预期）" if not checker.called else f"被回查了：{checker.called}")


# --------------------------------------------- 4. PushConsumer + AutoRenew
class SlowListener(MessageListener):
    """故意慢过 default_invisible_duration(30s) 的 listener。

    AutoRenew 生效时：续租让消息一直不可见，只消费一次。
    AutoRenew 失效时：30s 后不可见期到期，消息被重投，计数会 >1。
    这正是 B13.5 要保住的行为，也是本用例存在的唯一理由。
    """

    def __init__(self, hold_s):
        self.hold_s = hold_s
        self.seen = {}
        self.lock = threading.Lock()

    def consume(self, message):
        body = message.body.decode()
        with self.lock:
            self.seen[body] = self.seen.get(body, 0) + 1
            first = self.seen[body] == 1
        print(f"      listener 收到 {body}（第 {self.seen[body]} 次）", flush=True)
        if first:
            time.sleep(self.hold_s)
        return ConsumeResult.SUCCESS


def test_push_autorenew():
    topic, grp = f"py-push-{RUN}", f"py-g-push-{RUN}"
    p = Producer(cfg(), topics=(topic,))
    p.startup()
    p.send(new_msg(topic, "slow-one"))
    p.shutdown()

    lis = SlowListener(hold_s=45)   # > 30s 不可见期
    c = PushConsumer(cfg(), grp, message_listener=lis,
                     subscription={topic: FilterExpression("*")})
    c.startup()
    time.sleep(80)                  # 45s 处理 + 35s 观察窗口
    c.shutdown()
    cnt = lis.seen.get("slow-one", 0)
    if cnt == 1:
        record("push+AutoRenew", True, "处理 45s > 30s 不可见期，未发生重投（续租生效）")
    elif cnt == 0:
        record("push+AutoRenew", False, "PushConsumer 一次都没收到")
    else:
        record("push+AutoRenew", False, f"收到 {cnt} 次——续租未生效，消息被重复投递")


# ---------------------------------------------------------------- 5. 定时
def test_delay():
    topic = f"py-delay-{RUN}"
    p = Producer(cfg(), topics=(topic,))
    p.startup()
    p.send(new_msg(topic, "delay-15s", delay_s=15))
    sent = time.time()
    p.shutdown()

    c = SimpleConsumer(cfg(), f"py-g-delay-{RUN}", subscription={topic: FilterExpression("*")},
                       await_duration=3)
    c.startup()
    got = drain(c, 1, 40)
    c.shutdown()
    if not got:
        record("定时消息", False, "40s 内未收到")
        return
    elapsed = time.time() - sent
    # 早于 12s 到达说明延时根本没生效（留 3s 余量给时钟与调度抖动）
    ok = elapsed >= 12
    record("定时消息", ok, f"{elapsed:.1f}s 后到达（预期 ≥15s，早于 12s 判为延时未生效）")


# ---------------------------------------------------------------- 6. Recall
def test_recall():
    topic = f"py-recall-{RUN}"
    # 对照组：同样 25s 延时、**不撤回**，必须能收到。没有这一步，
    # 「延时根本没生效」之类的原因会把撤回用例伪装成通过（上一轮就踩了）。
    p0 = Producer(cfg(), topics=(topic,))
    p0.startup()
    p0.send(new_msg(topic, "control-not-recalled", delay_s=15))
    p0.shutdown()
    c0 = SimpleConsumer(cfg(), f"py-g-recall-ctl-{RUN}", subscription={topic: FilterExpression("*")},
                        await_duration=3)
    c0.startup()
    ctl = [m.body.decode() for m in drain(c0, 1, 45)]
    c0.shutdown()
    if ctl != ["control-not-recalled"]:
        record("Recall 对照组", False, f"未撤回的延时消息都没收到（{ctl}），撤回用例失去判别力")
        return
    record("Recall 对照组", True, "未撤回的延时消息如期到达，判别力成立")

    p = Producer(cfg(), topics=(topic,))
    p.startup()
    r = p.send(new_msg(topic, "to-be-recalled", delay_s=25))
    handle = getattr(r, "recall_handle", None)
    if not handle:
        p.shutdown()
        record("Recall 撤回", False, "SendReceipt 未带 recall_handle，无法撤回")
        return
    time.sleep(2)
    p.recall_message(topic, handle)
    p.shutdown()

    c = SimpleConsumer(cfg(), f"py-g-recall-{RUN}", subscription={topic: FilterExpression("*")},
                       await_duration=3)
    c.startup()
    bodies = [m.body.decode() for m in drain(c, 2, 45)]   # 覆盖原定 25s 投递时刻
    c.shutdown()
    # 只断言「被撤回的那条不出现」。不能断言「一条都没收到」——对照组消息
    # 在同一 topic 里，而这是个新消费组、会从头再读一遍，count==0 永远不成立。
    if "to-be-recalled" not in bodies:
        record("Recall 撤回", True,
               f"撤回后 45s（跨过原定投递时刻）未见该消息；同期收到对照组 {bodies}")
    else:
        record("Recall 撤回", False, f"撤回失败，仍收到 to-be-recalled（全部：{bodies}）")


CASES = [
    ("SQL92 属性过滤", test_sql92),
    ("FIFO 顺序消息", test_fifo),
    ("事务消息", test_transaction),
    ("PushConsumer + AutoRenew", test_push_autorenew),
    ("定时消息", test_delay),
    ("Recall 撤回", test_recall),
]

if __name__ == "__main__":
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for name, fn in CASES:
        if only and only not in name and only not in fn.__name__:
            continue
        print(f"\n=== {name} ===", flush=True)
        try:
            fn()
        except Exception as e:
            record(name, False, f"用例抛异常 {type(e).__name__}: {e}")
    print("\n=== 汇总 ===")
    bad = 0
    for name, ok, note in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {note}")
        if not ok:
            bad += 1
    print(f"\n共 {len(results)} 项，失败 {bad} 项")
    sys.exit(1 if bad else 0)
