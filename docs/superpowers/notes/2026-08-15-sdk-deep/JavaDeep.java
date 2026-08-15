// B13.3 深水区：官方 rocketmq-client-java 5.2.1 对 sq 的特性级验证。
//
// 职责：Java 是 RocketMQ 的参考实现，兼容性以它为准。逐项验证 SQL92 属性过滤、
//       FIFO 顺序、事务孤儿回查、PushConsumer + AutoRenew 续租、定时消息、
//       Recall 撤回。
// 边界：不测吞吐、不测鉴权、不测多消费者再均衡。
//
// 本轮最要紧的一项是 PushConsumer：Python SDK 因 sq 不发 ReceiveMessageResponse
// 信封层的 delivery_timestamp 而完全不可用（B13.8），C# 与 Go 则容忍。参考实现
// 落在哪一边，直接决定 B13.8 的定级。
//
// 判据设计原则：每项都必须有反例或对照组——只断言「该收的收到了」，一个恒不
// 过滤、或一个根本没投递的实现同样能「通过」。

import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.consumer.*;
import org.apache.rocketmq.client.apis.message.*;
import org.apache.rocketmq.client.apis.producer.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JavaDeep {
    static final String ENDPOINT = "127.0.0.1:28081";
    static final String ADMIN = "http://127.0.0.1:28082";
    // 每轮换一组 topic：topic 会跨轮次累积消息，而「新消费组从头读到了什么」
    // 这类断言依赖 topic 里只有本轮写入的数据。
    static final String RUN = String.valueOf(System.currentTimeMillis() % 1000000);

    static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    static final List<String[]> RESULTS = new ArrayList<>();

    static void record(String name, boolean ok, String note) {
        RESULTS.add(new String[]{name, ok ? "PASS" : "FAIL", note});
        System.out.printf("  [%s] %s: %s%n", ok ? "PASS" : "FAIL", name, note);
        System.out.flush();
    }

    static ClientConfiguration cfg() {
        return ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINT)
                // enableSsl(false) 是必须的：该 SDK 默认走 TLS，而 sq 监听的是明文
                // gRPC，不关掉就死在启动期的 QueryRoute 上，报
                // NotSslRecordException: not an SSL/TLS record。C# SDK 同款默认值。
                .enableSsl(false)
                .setRequestTimeout(Duration.ofSeconds(10))
                .build();
    }

    static Message msg(String topic, String body, Map<String, String> props,
                       String group, Long delayMs) {
        MessageBuilder b = PROVIDER.newMessageBuilder()
                .setTopic(topic).setBody(body.getBytes(StandardCharsets.UTF_8));
        if (props != null) props.forEach(b::addProperty);
        if (group != null) b.setMessageGroup(group);
        // 单位是毫秒（Java 侧无 Python 那种 setter 收秒/getter 返毫秒的不对称）
        if (delayMs != null) b.setDeliveryTimestamp(System.currentTimeMillis() + delayMs);
        return b.build();
    }

    static SimpleConsumer consumer(String group, String topic, FilterExpression fe) throws Exception {
        return PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(cfg()).setConsumerGroup(group)
                .setAwaitDuration(Duration.ofSeconds(2))
                .setSubscriptionExpressions(Collections.singletonMap(topic, fe))
                .build();
    }

    /** 在预算内尽量收满 want 条并逐条 ack。SimpleConsumer 每次 receive 只轮询一个
     *  队列，收不到不代表消息没到——必须轮询，不能单次 receive 就下结论。 */
    static List<String> drain(SimpleConsumer c, int want, int budgetSec) {
        List<String> got = new ArrayList<>();
        long deadline = System.currentTimeMillis() + budgetSec * 1000L;
        while (System.currentTimeMillis() < deadline && got.size() < want) {
            List<MessageView> batch;
            try {
                batch = c.receive(16, Duration.ofSeconds(10));
            } catch (Throwable e) {
                continue;   // 空轮询在该 SDK 里也走异常路径
            }
            for (MessageView mv : batch) {
                byte[] arr = new byte[mv.getBody().remaining()];
                mv.getBody().get(arr);
                got.add(new String(arr, StandardCharsets.UTF_8));
                try { c.ack(mv); } catch (Throwable ignored) { }
            }
        }
        return got;
    }

    // ------------------------------------------------------------- 1. SQL92
    static void testSql92() throws Exception {
        String topic = "j-sql92-" + RUN;
        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        p.send(msg(topic, "a-vip30-cn", Map.of("age", "30", "region", "cn", "vip", "true"), null, null));
        p.send(msg(topic, "b-vip15-cn", Map.of("age", "15", "region", "cn", "vip", "true"), null, null));
        p.send(msg(topic, "c-plain30-us", Map.of("age", "30", "region", "us", "vip", "false"), null, null));
        p.send(msg(topic, "d-noage-cn", Map.of("region", "cn"), null, null));   // age 缺失
        p.send(msg(topic, "e-vip22-cn", Map.of("age", "22", "region", "cn", "vip", "true"), null, null));
        p.close();

        // 命中 a、e。反例：b(年龄小) c(vip 否) d(age 缺失→UNKNOWN)
        String expr = "age > 18 AND age <= 50 AND region IN ('cn','us') AND vip = 'true'";
        SimpleConsumer c = consumer("j-g-sql92-" + RUN, topic,
                new FilterExpression(expr, FilterExpressionType.SQL92));
        Set<String> got = new TreeSet<>(drain(c, 5, 40));
        c.close();
        Set<String> want = new TreeSet<>(List.of("a-vip30-cn", "e-vip22-cn"));
        record("sql92-复合表达式", got.equals(want),
                got.equals(want) ? "命中 " + got + "，3 条反例均正确排除" : "期望 " + want + "，实得 " + got);

        // 三值逻辑：age 缺失时 NOT(age > 18) 不得为真。这是 spec §2.2 明确定的语义，
        // 也是二值逻辑实现最容易踩错的一条——d 出现即为缺陷。
        SimpleConsumer c2 = consumer("j-g-sql92-not-" + RUN, topic,
                new FilterExpression("NOT (age > 18)", FilterExpressionType.SQL92));
        Set<String> got2 = new TreeSet<>(drain(c2, 5, 30));
        c2.close();
        boolean ok2 = got2.equals(new TreeSet<>(List.of("b-vip15-cn")));
        record("sql92-三值逻辑", ok2,
                ok2 ? "age 缺失的消息未因 NOT 被误投；仅 b(age=15) 命中"
                    : "期望仅 [b-vip15-cn]，实得 " + got2 + "（d 出现即为二值逻辑缺陷）");

        SimpleConsumer c3 = consumer("j-g-sql92-isnull-" + RUN, topic,
                new FilterExpression("age IS NULL", FilterExpressionType.SQL92));
        Set<String> got3 = new TreeSet<>(drain(c3, 5, 30));
        c3.close();
        boolean ok3 = got3.equals(new TreeSet<>(List.of("d-noage-cn")));
        record("sql92-IS NULL", ok3, ok3 ? "精确选出 d-noage-cn" : "实得 " + got3);
    }

    // -------------------------------------------------------------- 2. FIFO
    static void testFifo() throws Exception {
        String topic = "j-fifo-" + RUN;
        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        int n = 10;
        for (int i = 0; i < n; i++) {
            p.send(msg(topic, String.format("seq-%02d", i), null, "j-order-9", null));
        }
        p.close();

        SimpleConsumer c = consumer("j-g-fifo-" + RUN, topic, new FilterExpression("*"));
        List<String> got = drain(c, n, 90);
        c.close();
        List<String> want = new ArrayList<>();
        for (int i = 0; i < n; i++) want.add(String.format("seq-%02d", i));
        if (got.equals(want)) {
            record("fifo-同组顺序", true, n + " 条按发送序到达");
        } else if (got.size() < n) {
            record("fifo-同组顺序", false, "90s 内只收到 " + got.size() + "/" + n + " 条：" + got);
        } else {
            record("fifo-同组顺序", false, "顺序不符：" + got);
        }
    }

    // ---------------------------------------------------- 3. 事务孤儿回查
    static final class Checker implements TransactionChecker {
        final List<String> checked = Collections.synchronizedList(new ArrayList<>());

        @Override
        public TransactionResolution check(MessageView mv) {
            byte[] arr = new byte[mv.getBody().remaining()];
            mv.getBody().get(arr);
            String body = new String(arr, StandardCharsets.UTF_8);
            checked.add(body);
            System.out.println("      回查触发：" + body
                    + " → " + (body.contains("commit") ? "COMMIT" : "ROLLBACK"));
            return body.contains("commit") ? TransactionResolution.COMMIT : TransactionResolution.ROLLBACK;
        }
    }

    static void testTransactionCheck() throws Exception {
        String topic = "j-txn-" + RUN;
        Checker checker = new Checker();
        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).setTransactionChecker(checker).build();

        // 半消息发出后**不显式裁决**，让它变成孤儿；broker 到点回查，
        // checker 按消息体决定提交还是回滚。
        Transaction t1 = p.beginTransaction();
        p.send(msg(topic, "orphan-commit", null, null, null), t1);
        Transaction t2 = p.beginTransaction();
        p.send(msg(topic, "orphan-rollback", null, null, null), t2);

        System.out.println("      等待 broker 回查（txn_check_interval=5s）…");
        Thread.sleep(40_000);

        SimpleConsumer c = consumer("j-g-txn-" + RUN, topic, new FilterExpression("*"));
        Set<String> got = new TreeSet<>(drain(c, 2, 30));
        c.close();
        p.close();

        record("事务-回查被触发", !checker.checked.isEmpty(),
                checker.checked.isEmpty() ? "40s 内 broker 一次都没回查——孤儿半消息不会被解决"
                        : "broker 回查了 " + checker.checked.size() + " 次："
                          + new TreeSet<>(checker.checked));
        boolean ok = got.equals(new TreeSet<>(List.of("orphan-commit")));
        record("事务-回查裁决生效", ok,
                ok ? "COMMIT 的可见、ROLLBACK 的不可见" : "期望仅 [orphan-commit]，实得 " + got);
    }

    // ------------------------------------- 4. PushConsumer + AutoRenew
    static final class SlowListener implements MessageListener {
        final int holdSec;
        final Map<String, AtomicInteger> seen = new ConcurrentHashMap<>();

        SlowListener(int holdSec) { this.holdSec = holdSec; }

        @Override
        public ConsumeResult consume(MessageView mv) {
            byte[] arr = new byte[mv.getBody().remaining()];
            mv.getBody().get(arr);
            String body = new String(arr, StandardCharsets.UTF_8);
            int n = seen.computeIfAbsent(body, k -> new AtomicInteger()).incrementAndGet();
            System.out.println("      listener 收到 " + body + "（第 " + n + " 次）");
            System.out.flush();
            if (n == 1) {
                try { Thread.sleep(holdSec * 1000L); } catch (InterruptedException ignored) { }
            }
            return ConsumeResult.SUCCESS;
        }
    }

    static void testPushAutoRenew() throws Exception {
        String topic = "j-push-" + RUN;
        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        p.send(msg(topic, "j-slow-one", null, null, null));
        p.close();

        // 处理耗时故意超过 broker 的 default_invisible_duration(30s)：
        // 续租生效则只消费一次；失效则 30s 后重投，计数 >1。B13.5 的承重判据。
        SlowListener lis = new SlowListener(45);
        PushConsumer pc = PROVIDER.newPushConsumerBuilder()
                .setClientConfiguration(cfg()).setConsumerGroup("j-g-push-" + RUN)
                .setSubscriptionExpressions(Collections.singletonMap(topic, new FilterExpression("*")))
                .setMessageListener(lis).build();
        Thread.sleep(80_000);   // 45s 处理 + 35s 观察窗
        pc.close();

        AtomicInteger cnt = lis.seen.get("j-slow-one");
        int n = cnt == null ? 0 : cnt.get();
        record("push+AutoRenew", n == 1,
                n == 1 ? "处理 45s > 30s 不可见期，未重投（续租生效）"
                        : n == 0 ? "PushConsumer 一次都没收到——与 Python 同症状，见 B13.8"
                                 : "收到 " + n + " 次——续租未生效，消息被重复投递");
    }

    // -------------------------------------------------------------- 5. 定时
    static void testDelay() throws Exception {
        String topic = "j-delay-" + RUN;
        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        long sent = System.currentTimeMillis();
        p.send(msg(topic, "delay-15s", null, null, 15_000L));
        p.close();

        SimpleConsumer c = consumer("j-g-delay-" + RUN, topic, new FilterExpression("*"));
        List<String> got = drain(c, 1, 45);
        c.close();
        if (got.isEmpty()) {
            record("定时消息", false, "45s 内未收到");
            return;
        }
        long elapsed = System.currentTimeMillis() - sent;
        // 早于 12s 到达说明延时根本没生效（留 3s 余量给时钟与调度抖动）
        record("定时消息", elapsed >= 12_000,
                String.format("%.1fs 后到达（预期 ≥15s，早于 12s 判为延时未生效）", elapsed / 1000.0));
    }

    // ------------------------------------------------------------ 6. Recall
    static void testRecall() throws Exception {
        String topic = "j-recall-" + RUN;
        // 对照组：同样延时、**不撤回**，必须能收到。少了这一步，任何让消息不到的
        // 原因都会把撤回用例伪装成通过（Python 那轮就踩过）。
        Producer p0 = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        p0.send(msg(topic, "control-not-recalled", null, null, 15_000L));
        p0.close();
        SimpleConsumer c0 = consumer("j-g-recall-ctl-" + RUN, topic, new FilterExpression("*"));
        List<String> ctl = drain(c0, 1, 45);
        c0.close();
        if (!ctl.equals(List.of("control-not-recalled"))) {
            record("Recall 对照组", false, "未撤回的延时消息都没收到（" + ctl + "），撤回用例失去判别力");
            return;
        }
        record("Recall 对照组", true, "未撤回的延时消息如期到达，判别力成立");

        Producer p = PROVIDER.newProducerBuilder().setClientConfiguration(cfg())
                .setTopics(topic).build();
        SendReceipt r = p.send(msg(topic, "to-be-recalled", null, null, 25_000L));
        String handle = r.getRecallHandle();
        if (handle == null || handle.isEmpty()) {
            p.close();
            record("Recall 撤回", false, "SendReceipt 未带 recallHandle");
            return;
        }
        Thread.sleep(2000);
        RecallReceipt rr = p.recallMessage(topic, handle);
        System.out.println("      撤回回执 msgId=" + rr.getMessageId());
        p.close();

        SimpleConsumer c = consumer("j-g-recall-" + RUN, topic, new FilterExpression("*"));
        List<String> bodies = drain(c, 2, 45);   // 跨过原定 25s 投递时刻
        c.close();
        // 只断言「被撤回的那条不出现」。不能断言「一条都没收到」——对照组消息在
        // 同一 topic 里，而这是个新消费组、会从头再读一遍。
        boolean recalled = bodies.contains("to-be-recalled");
        record("Recall 撤回", !recalled,
                !recalled ? "撤回后 45s（跨过原定投递时刻）未见该消息；同期收到对照组 " + bodies
                          : "撤回失败，仍收到 to-be-recalled（全部：" + bodies + "）");
    }

    public static void main(String[] args) throws Exception {
        String only = args.length > 0 ? args[0] : null;
        Map<String, RunnableEx> cases = new LinkedHashMap<>();
        cases.put("SQL92 属性过滤", JavaDeep::testSql92);
        cases.put("FIFO 顺序消息", JavaDeep::testFifo);
        cases.put("事务孤儿回查", JavaDeep::testTransactionCheck);
        cases.put("PushConsumer + AutoRenew", JavaDeep::testPushAutoRenew);
        cases.put("定时消息", JavaDeep::testDelay);
        cases.put("Recall 撤回", JavaDeep::testRecall);

        for (Map.Entry<String, RunnableEx> e : cases.entrySet()) {
            if (only != null && !e.getKey().contains(only)) continue;
            System.out.println("\n=== " + e.getKey() + " ===");
            System.out.flush();
            try {
                e.getValue().run();
            } catch (Throwable t) {
                record(e.getKey(), false, "用例抛异常 " + t.getClass().getSimpleName() + ": " + t.getMessage());
            }
        }

        System.out.println("\n=== 汇总 ===");
        int bad = 0;
        for (String[] r : RESULTS) {
            System.out.printf("  %s  %s: %s%n", r[1], r[0], r[2]);
            if (r[1].equals("FAIL")) bad++;
        }
        System.out.printf("%n共 %d 项，失败 %d 项%n", RESULTS.size(), bad);
        System.exit(bad == 0 ? 0 : 1);
    }

    interface RunnableEx { void run() throws Exception; }
}
