// B13.3 深水区：官方 RocketMQ.Client 5.2.1 对 sq 的特性级验证。
//
// 职责：用官方 C# SDK 验证 sq 的消息特性，重点覆盖 Python 侧够不到的角度——
//       事务回查（ITransactionChecker 的孤儿半消息恢复）——并对 SQL92、FIFO、
//       PushConsumer+AutoRenew、Recall 做跨语言互证。
// 边界：不测吞吐、不测鉴权（08-14 已单独冒烟）。
//
// 每项都带反例：只断言「该收的收到了」，一个恒不过滤的实现也能通过。

using System.Collections.Concurrent;
using Org.Apache.Rocketmq;

const string Endpoint = "172.19.25.180:28081";
// 每轮换一组 topic：topic 跨轮次累积会让「新消费组从头读」类断言失真
// （上一轮 Recall 就因此把对照组消息误判成撤回失败）。
string Run = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString()[^6..];
var results = new List<(string name, bool ok, string note)>();

void Record(string name, bool ok, string note)
{
    results.Add((name, ok, note));
    Console.WriteLine($"  [{(ok ? "PASS" : "FAIL")}] {name}: {note}");
}

// EnableSsl(false) 是必须的：该 SDK 默认走 TLS，而 sq 监听明文 gRPC，
// 不关掉握手就死在 QueryRoute 上（AuthenticationException: Cannot determine
// the frame size）。这条 08-14 冒烟时已踩过一次。
ClientConfig Cfg() => new ClientConfig.Builder().SetEndpoints(Endpoint).EnableSsl(false).Build();

Message Msg(string topic, string body, string? tag = null,
            Dictionary<string, string>? props = null, string? group = null, long delayMs = 0)
{
    var b = new Message.Builder().SetTopic(topic).SetBody(System.Text.Encoding.UTF8.GetBytes(body));
    if (tag != null) b.SetTag(tag);
    if (props != null) foreach (var kv in props) b.AddProperty(kv.Key, kv.Value);
    if (group != null) b.SetMessageGroup(group);
    if (delayMs > 0) b.SetDeliveryTimestamp(DateTime.UtcNow.AddMilliseconds(delayMs));
    return b.Build();
}

// Drain 在预算内尽量收满 want 条。SimpleConsumer 每次 Receive 只轮询一个队列，
// 收不到不代表消息没到——必须轮询，不能单次 Receive 就下结论。
async Task<List<MessageView>> Drain(SimpleConsumer c, int want, int budgetSec)
{
    var got = new List<MessageView>();
    var deadline = DateTime.UtcNow.AddSeconds(budgetSec);
    while (DateTime.UtcNow < deadline && got.Count < want)
    {
        List<MessageView> batch;
        try { batch = await c.Receive(16, TimeSpan.FromSeconds(10)); }
        catch { continue; }   // 空轮询在该 SDK 里也走异常路径
        foreach (var mv in batch) { got.Add(mv); await c.Ack(mv); }
    }
    return got;
}

async Task<SimpleConsumer> NewConsumer(string group, string topic, FilterExpression fe) =>
    await new SimpleConsumer.Builder().SetClientConfig(Cfg()).SetConsumerGroup(group)
        .SetAwaitDuration(TimeSpan.FromSeconds(3))
        .SetSubscriptionExpression(new Dictionary<string, FilterExpression> { { topic, fe } })
        .Build();

// ------------------------------------------------------------------ 1. SQL92
async Task TestSql92()
{
    string topic = "cs-sql92-" + Run;
    var p = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic).Build();
    await p.Send(Msg(topic, "hit-1", props: new() { { "lvl", "5" }, { "env", "prod" } }));
    await p.Send(Msg(topic, "miss-lowlvl", props: new() { { "lvl", "1" }, { "env", "prod" } }));
    await p.Send(Msg(topic, "miss-env", props: new() { { "lvl", "7" }, { "env", "dev" } }));
    await p.Send(Msg(topic, "hit-2", props: new() { { "lvl", "9" }, { "env", "prod" } }));
    await p.DisposeAsync();

    var fe = new FilterExpression("lvl BETWEEN 4 AND 10 AND env = 'prod'", ExpressionType.Sql92);
    var c = await NewConsumer("cs-g-sql92-" + Run, topic, fe);
    var got = (await Drain(c, 4, 40)).Select(m => System.Text.Encoding.UTF8.GetString(m.Body)).ToHashSet();
    await c.DisposeAsync();

    var want = new HashSet<string> { "hit-1", "hit-2" };
    Record("sql92-BETWEEN+等值", got.SetEquals(want),
        got.SetEquals(want) ? "命中 hit-1/hit-2，2 条反例正确排除"
                            : $"期望 [hit-1,hit-2]，实得 [{string.Join(",", got.OrderBy(x => x))}]");
}

// ------------------------------------------------------------------- 2. FIFO
async Task TestFifo()
{
    string topic = "cs-fifo-" + Run;
    var p = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic).Build();
    for (int i = 0; i < 10; i++) await p.Send(Msg(topic, $"seq-{i:D2}", group: "cs-order-7"));
    await p.DisposeAsync();

    var c = await NewConsumer("cs-g-fifo-" + Run, topic, new FilterExpression("*"));
    var got = (await Drain(c, 10, 60)).Select(m => System.Text.Encoding.UTF8.GetString(m.Body)).ToList();
    await c.DisposeAsync();

    var want = Enumerable.Range(0, 10).Select(i => $"seq-{i:D2}").ToList();
    Record("fifo-同组顺序", got.SequenceEqual(want),
        got.SequenceEqual(want) ? "10 条按发送序到达" : $"顺序不符：[{string.Join(",", got)}]");
}

async Task TestTransactionCheck()
{
    string topic = "cs-txn-check-" + Run;
    var checker = new Checker();
    var p = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic)
        .SetTransactionChecker(checker).Build();

    var t1 = p.BeginTransaction();
    await p.Send(Msg(topic, "orphan-commit"), t1);   // 故意不 Commit()
    var t2 = p.BeginTransaction();
    await p.Send(Msg(topic, "orphan-rollback"), t2); // 故意不 Rollback()

    // broker 的 txn_check_interval 配的是 5s；给足几轮回查窗口
    Console.WriteLine("      等待 broker 回查（txn_check_interval=5s）…");
    await Task.Delay(TimeSpan.FromSeconds(40));

    var c = await NewConsumer("cs-g-txn-check-" + Run, topic, new FilterExpression("*"));
    var got = (await Drain(c, 2, 30)).Select(m => System.Text.Encoding.UTF8.GetString(m.Body)).ToHashSet();
    await c.DisposeAsync();
    await p.DisposeAsync();

    var checkedAny = checker.Checked.Count > 0;
    Record("事务-回查被触发", checkedAny,
        checkedAny ? $"broker 回查了 {checker.Checked.Count} 次：[{string.Join(",", checker.Checked.Distinct())}]"
                   : "40s 内 broker 一次都没回查——孤儿半消息不会被解决");
    var want = new HashSet<string> { "orphan-commit" };
    Record("事务-回查裁决生效", got.SetEquals(want),
        got.SetEquals(want) ? "COMMIT 的可见、ROLLBACK 的不可见"
                            : $"期望仅 [orphan-commit]，实得 [{string.Join(",", got.OrderBy(x => x))}]");
}

async Task TestPushAutoRenew()
{
    string topic = "cs-push-" + Run;
    var p = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic).Build();
    await p.Send(Msg(topic, "cs-slow-one"));
    await p.DisposeAsync();

    var lis = new SlowListener(45);
    var pc = await new PushConsumer.Builder().SetClientConfig(Cfg()).SetConsumerGroup("cs-g-push-" + Run)
        .SetSubscriptionExpression(new Dictionary<string, FilterExpression> { { topic, new FilterExpression("*") } })
        .SetMessageListener(lis).Build();
    await Task.Delay(TimeSpan.FromSeconds(80));   // 45s 处理 + 35s 观察窗
    await pc.DisposeAsync();

    lis.Seen.TryGetValue("cs-slow-one", out var cnt);
    Record("push+AutoRenew", cnt == 1,
        cnt == 1 ? "处理 45s > 30s 不可见期，未重投（续租生效）"
                 : cnt == 0 ? "PushConsumer 一次都没收到" : $"收到 {cnt} 次——续租未生效");
}

// ----------------------------------------------------------------- 5. Recall
async Task TestRecall()
{
    string topic = "cs-recall-" + Run;
    // 对照组：同样延时、**不撤回**，必须能收到。少了这一步，任何让消息不到的
    // 原因（延时参数写错、topic 路由错）都会把撤回用例伪装成通过。
    var p0 = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic).Build();
    await p0.Send(Msg(topic, "cs-control-not-recalled", delayMs: 15000));
    await p0.DisposeAsync();
    var c0 = await NewConsumer("cs-g-recall-ctl-" + Run, topic, new FilterExpression("*"));
    var ctl = (await Drain(c0, 1, 45)).Select(m => System.Text.Encoding.UTF8.GetString(m.Body)).ToList();
    await c0.DisposeAsync();
    if (ctl.Count != 1 || ctl[0] != "cs-control-not-recalled")
    {
        Record("Recall 对照组", false, $"未撤回的延时消息都没收到（[{string.Join(",", ctl)}]），撤回用例失去判别力");
        return;
    }
    Record("Recall 对照组", true, "未撤回的延时消息如期到达，判别力成立");

    var p = await new Producer.Builder().SetClientConfig(Cfg()).SetTopics(topic).Build();
    var receipt = await p.Send(Msg(topic, "cs-to-recall", delayMs: 25000));
    if (string.IsNullOrEmpty(receipt.RecallHandle))
    {
        await p.DisposeAsync();
        Record("Recall 撤回", false, "SendReceipt 未带 RecallHandle");
        return;
    }
    await Task.Delay(2000);
    var rr = await p.RecallMessage(topic, receipt.RecallHandle);
    Console.WriteLine($"      撤回回执 msgId={rr}");
    await p.DisposeAsync();

    var c = await NewConsumer("cs-g-recall-" + Run, topic, new FilterExpression("*"));
    var bodies = (await Drain(c, 2, 45)).Select(m => System.Text.Encoding.UTF8.GetString(m.Body)).ToList();
    await c.DisposeAsync();
    // 只断言「被撤回的那条不出现」。不能断言「一条都没收到」——对照组消息在
    // 同一 topic 里，而这是个新消费组、会从头再读一遍，Count==0 永远不成立。
    var recalled = bodies.Contains("cs-to-recall");
    Record("Recall 撤回", !recalled,
        !recalled ? $"撤回后 45s（跨过原定投递时刻）未见该消息；同期收到对照组 [{string.Join(",", bodies)}]"
                  : $"撤回失败，仍收到 cs-to-recall（全部：[{string.Join(",", bodies)}]）");
}

// ------------------------------------------------------------------- 主流程
var cases = new (string name, Func<Task> fn)[]
{
    ("SQL92 属性过滤", TestSql92),
    ("FIFO 顺序消息", TestFifo),
    ("事务回查", TestTransactionCheck),
    ("PushConsumer + AutoRenew", TestPushAutoRenew),
    ("Recall 撤回", TestRecall),
};

var only = args.Length > 0 ? args[0] : null;
foreach (var (name, fn) in cases)
{
    if (only != null && !name.Contains(only) && !fn.Method.Name.Contains(only)) continue;
    Console.WriteLine($"\n=== {name} ===");
    try { await fn(); }
    catch (Exception e) { Record(name, false, $"用例抛异常 {e.GetType().Name}: {e.Message}"); }
}

Console.WriteLine("\n=== 汇总 ===");
var bad = 0;
foreach (var (name, ok, note) in results)
{
    Console.WriteLine($"  {(ok ? "PASS" : "FAIL")}  {name}: {note}");
    if (!ok) bad++;
}
Console.WriteLine($"\n共 {results.Count} 项，失败 {bad} 项");
return bad == 0 ? 0 : 1;

// ===================== 类型声明（C# 要求顶层语句先于类型声明，故置于文件末尾）

// ------------------------------------------- 3. 事务回查（本文件的重点）
// 半消息发出后**不显式 commit/rollback**，让它变成孤儿；broker 到点回查，
// checker 按消息 tag 决定提交还是回滚。这条路径 Python 侧的显式提交测不到。
class Checker : ITransactionChecker
{
    public readonly ConcurrentBag<string> Checked = new();
    public TransactionResolution Check(MessageView mv)
    {
        var body = System.Text.Encoding.UTF8.GetString(mv.Body);
        Checked.Add(body);
        Console.WriteLine($"      回查触发：{body} → {(body.Contains("commit") ? "COMMIT" : "ROLLBACK")}");
        return body.Contains("commit") ? TransactionResolution.Commit : TransactionResolution.Rollback;
    }
}

// --------------------------------------- 4. PushConsumer + AutoRenew
// 处理耗时故意超过 broker 的 default_invisible_duration(30s)：
// 续租生效则只消费一次；失效则 30s 后重投，计数 >1。这是 B13.5 的承重判据。
class SlowListener : IMessageListener
{
    readonly int _holdSec;
    public readonly ConcurrentDictionary<string, int> Seen = new();
    public SlowListener(int holdSec) => _holdSec = holdSec;
    public ConsumeResult Consume(MessageView mv)
    {
        var body = System.Text.Encoding.UTF8.GetString(mv.Body);
        var n = Seen.AddOrUpdate(body, 1, (_, v) => v + 1);
        Console.WriteLine($"      listener 收到 {body}（第 {n} 次）");
        if (n == 1) Thread.Sleep(TimeSpan.FromSeconds(_holdSec));
        return ConsumeResult.SUCCESS;
    }
}
