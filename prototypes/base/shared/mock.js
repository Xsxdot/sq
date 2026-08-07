/*
 * sq 控制台原型 · 共享 mock 数据与渲染工具
 *
 * 职责：
 *   - 提供全站唯一一份假数据（topic / 消费组 / 消费关系 / 消息 / 死信 / 延时 / 事务）
 *   - 提供 offset 带、迷你折线、数字与时间格式化等跨页复用的渲染函数
 *
 * 边界：
 *   - 不发任何真实请求；字段名刻意对齐 Admin API 的返回结构，
 *     方便后续真实页面照着替换数据源而不用改渲染逻辑
 *   - 不做路由或状态管理，页面之间靠相对链接 + query 参数传递
 *   - 用固定基准时间而不是 Date.now()，保证每次打开页面数据一致、便于比对
 */

window.SQ = (function () {
  // 固定基准时刻：2026-08-06 13:40:00，避免每次刷新时间显示都在跳
  const NOW = new Date('2026-08-06T13:40:00').getTime();

  const topics = [
    { name: 'order.created', queues: 4, retentionMs: 259200000, createdAtMs: NOW - 86400000 * 12,
      queuesDetail: [{ queueId: 0, nextOffset: 321122 }, { queueId: 1, nextOffset: 321130 }, { queueId: 2, nextOffset: 321124 }, { queueId: 3, nextOffset: 321126 }] },
    { name: 'order.paid', queues: 2, retentionMs: 259200000, createdAtMs: NOW - 86400000 * 12,
      queuesDetail: [{ queueId: 0, nextOffset: 201058 }, { queueId: 1, nextOffset: 201073 }] },
    { name: 'sms.notify', queues: 3, retentionMs: 86400000, createdAtMs: NOW - 86400000 * 5,
      queuesDetail: [{ queueId: 0, nextOffset: 32294 }, { queueId: 1, nextOffset: 32293 }, { queueId: 2, nextOffset: 32295 }] },
    { name: 'audit.log', queues: 2, retentionMs: 604800000, createdAtMs: NOW - 86400000 * 30,
      queuesDetail: [{ queueId: 0, nextOffset: 1105220 }, { queueId: 1, nextOffset: 1105227 }] },
    { name: 'delay.remind', queues: 1, retentionMs: 172800000, createdAtMs: NOW - 86400000 * 3,
      queuesDetail: [{ queueId: 0, nextOffset: 15412 }] },
  ];

  const groups = [
    { name: 'order-svc', maxAttempts: 16, createdAtMs: NOW - 86400000 * 12 },
    { name: 'notify-svc', maxAttempts: 5, createdAtMs: NOW - 86400000 * 5 },
    { name: 'audit-svc', maxAttempts: 16, createdAtMs: NOW - 86400000 * 30 },
  ];

  // 一条消费关系 = 一个消费组在一个 topic 上的进度。
  // cursor 是组的提交位点，head 是写入头，两者之差即落后量，其中 fly 条已发出待确认。
  const consumption = [
    { group: 'order-svc', topic: 'order.created', cursor: 1284310, head: 1284502, fly: 24, dlq: 0, qps: 1820, lastMs: NOW - 2000,
      queues: [[0, 321070, 321122], [1, 321084, 321130], [2, 321078, 321124], [3, 321078, 321126]] },
    { group: 'order-svc', topic: 'order.paid', cursor: 402118, head: 402131, fly: 6, dlq: 0, qps: 610, lastMs: NOW - 3000,
      queues: [[0, 201050, 201058], [1, 201068, 201073]] },
    { group: 'notify-svc', topic: 'sms.notify', cursor: 91204, head: 96882, fly: 128, dlq: 37, qps: 340, lastMs: NOW - 240000,
      queues: [[0, 30401, 32294], [1, 30398, 32293], [2, 30405, 32295]] },
    { group: 'audit-svc', topic: 'audit.log', cursor: 2210447, head: 2210447, fly: 0, dlq: 0, qps: 77, lastMs: NOW - 1000,
      queues: [[0, 1105220, 1105220], [1, 1105227, 1105227]] },
    { group: 'notify-svc', topic: 'delay.remind', cursor: 15330, head: 15412, fly: 12, dlq: 2, qps: 0, lastMs: NOW - 3600000,
      queues: [[0, 15330, 15412]] },
  ];

  const messages = [
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHA1', topic: 'order.created', queueId: 1, offset: 321129, keys: 'ORD-20260806-8842', tag: 'created', type: 'NORMAL', bornAtMs: NOW - 4000, storeAtMs: NOW - 3990, body: '{"orderId":"ORD-20260806-8842","userId":90218,"amount":29900,"items":3}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHA2', topic: 'order.created', queueId: 0, offset: 321121, keys: 'ORD-20260806-8841', tag: 'created', type: 'NORMAL', bornAtMs: NOW - 9000, storeAtMs: NOW - 8991, body: '{"orderId":"ORD-20260806-8841","userId":11907,"amount":8800,"items":1}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHB7', topic: 'sms.notify', queueId: 2, offset: 32294, keys: 'ORD-20260806-8842', tag: 'sms', type: 'NORMAL', bornAtMs: NOW - 3800, storeAtMs: NOW - 3795, body: '{"mobile":"138****2210","tpl":"order_paid","orderId":"ORD-20260806-8842"}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHC3', topic: 'delay.remind', queueId: 0, offset: 15411, keys: 'ORD-20260806-8830', tag: 'remind', type: 'DELAY', bornAtMs: NOW - 1800000, storeAtMs: NOW - 1799000, deliverAtMs: NOW + 1800000, body: '{"orderId":"ORD-20260806-8830","action":"unpaid_remind"}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHD9', topic: 'order.paid', queueId: 1, offset: 201072, keys: 'ORD-20260806-8839', tag: 'paid', type: 'FIFO', group: 'ORD-20260806-8839', bornAtMs: NOW - 12000, storeAtMs: NOW - 11994, body: '{"orderId":"ORD-20260806-8839","payChannel":"alipay","amount":15600}' },
  ];

  // 死信条目保留了来源坐标（M2 写入的 sq-origin-* 属性），重发据此回原 topic
  const dlq = [
    { group: 'notify-svc', queueId: 0, offset: 36, msgId: '01F8MECHZX3TBDSZ7XR8H8JG10', keys: 'ORD-20260806-8712', originTopic: 'sms.notify', originQueue: 1, originOffset: 30887, attempts: 5, lastError: 'consumer 未在 30s 内 ack，超过 max_attempts=5', storeAtMs: NOW - 600000, body: '{"mobile":"139****7781","tpl":"order_paid","orderId":"ORD-20260806-8712"}' },
    { group: 'notify-svc', queueId: 0, offset: 35, msgId: '01F8MECHZX3TBDSZ7XR8H8JG09', keys: 'ORD-20260806-8709', originTopic: 'sms.notify', originQueue: 0, originOffset: 30880, attempts: 5, lastError: 'consumer 未在 30s 内 ack，超过 max_attempts=5', storeAtMs: NOW - 640000, body: '{"mobile":"137****1102","tpl":"order_paid","orderId":"ORD-20260806-8709"}' },
    { group: 'notify-svc', queueId: 0, offset: 34, msgId: '01F8MECHZX3TBDSZ7XR8H8JG08', keys: 'ORD-20260806-8701', originTopic: 'delay.remind', originQueue: 0, originOffset: 15288, attempts: 5, lastError: '顺序消息卡住超过 max_attempts=5，整队解锁', storeAtMs: NOW - 900000, body: '{"orderId":"ORD-20260806-8701","action":"unpaid_remind"}' },
  ];

  const delay = [
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHC3', topic: 'delay.remind', deliverAtMs: NOW + 120000, keys: 'ORD-20260806-8830', body: '{"orderId":"ORD-20260806-8830","action":"unpaid_remind"}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHC4', topic: 'delay.remind', deliverAtMs: NOW + 900000, keys: 'ORD-20260806-8834', body: '{"orderId":"ORD-20260806-8834","action":"unpaid_remind"}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHC5', topic: 'order.created', deliverAtMs: NOW + 1800000, keys: 'ORD-20260806-8836', body: '{"orderId":"ORD-20260806-8836","action":"auto_close"}' },
    { msgId: '01F8MECHZX3TBDSZ7XR8H8JHC6', topic: 'delay.remind', deliverAtMs: NOW + 3600000, keys: 'ORD-20260806-8840', body: '{"orderId":"ORD-20260806-8840","action":"unpaid_remind"}' },
  ];

  // 待决事务（M6）：字段对齐 GET /admin/transactions 的返回结构
  // （txId/msgId/nextCheckMs/bornMs 为 camelCase 对应 tx_id/msg_id/next_check_ms/born_ms）。
  // 造数据时给一条回查次数较高的（check 11），让「已回查」列看得见数值
  const transactions = [
    { txId: '01F8MECHZX3TBDSZ7XR8H8JHC0', msgId: '01F8MECHZX3TBDSZ7XR8H8JHB1', topic: 'order.created', nextCheckMs: NOW + 30000, checks: 1, bornMs: NOW - 60000 },
    { txId: '01F8MECHZX3TBDSZ7XR8H8JHC1', msgId: '01F8MECHZX3TBDSZ7XR8H8JHB2', topic: 'delay.remind', nextCheckMs: NOW + 600000, checks: 11, bornMs: NOW - 5400000 },
    { txId: '01F8MECHZX3TBDSZ7XR8H8JHC2', msgId: '01F8MECHZX3TBDSZ7XR8H8JHB3', topic: 'sms.notify', nextCheckMs: NOW + 900000, checks: 0, bornMs: NOW - 4000 },
    { txId: '01F8MECHZX3TBDSZ7XR8H8JHC3', msgId: '01F8MECHZX3TBDSZ7XR8H8JHB4', topic: 'order.created', nextCheckMs: NOW + 1800000, checks: 2, bornMs: NOW - 120000 },
  ];

  const overview = { qps: 2847, qpsPeak1h: 4180, lag: 5965, inflight: 170, delayDepth: 1204, halfDepth: 3, dlq: 39, connections: 12, topics: 5, groups: 3 };

  /* ---------------- 工具函数 ---------------- */

  const fmt = n => Number(n).toLocaleString('en-US');

  // 相对时间：排查时「4 分钟前」比绝对时间戳更快形成判断
  function ago(ms) {
    const d = Math.max(0, Math.round((NOW - ms) / 1000));
    if (d < 5) return '刚刚';
    if (d < 60) return `${d}s 前`;
    if (d < 3600) return `${Math.round(d / 60)}m 前`;
    if (d < 86400) return `${Math.round(d / 3600)}h 前`;
    return `${Math.round(d / 86400)}d 前`;
  }

  function until(ms) {
    const d = Math.max(0, Math.round((ms - NOW) / 1000));
    if (d < 60) return `${d}s 后`;
    if (d < 3600) return `${Math.round(d / 60)}m 后`;
    return `${Math.round(d / 3600)}h 后`;
  }

  const time = ms => new Date(ms).toLocaleString('zh-CN', { hour12: false });
  const dur = ms => ms % 86400000 === 0 ? `${ms / 86400000} 天` : `${Math.round(ms / 3600000)} 小时`;

  // 固定波形，保证每次打开曲线一致，便于两个方向或前后改动之间比对
  function wave(n, base, amp, seed) {
    const out = [];
    for (let i = 0; i < n; i++) {
      const t = i / n;
      out.push(Math.max(0, Math.round(base + Math.sin(t * 9 + seed) * amp * .55 + Math.sin(t * 21 + seed * 2) * amp * .3)));
    }
    return out;
  }

  function linePath(vals, w, h, pad) {
    const max = Math.max(...vals, 1);
    return vals.map((v, i) => {
      const x = pad + (w - pad * 2) * (i / (vals.length - 1));
      const y = h - pad - (h - pad * 2) * (v / max);
      return `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`;
    }).join('');
  }

  // 迷你折线：尺寸从 svg 自身的 width/height 属性读，调用点不必重复传
  function spark(el, vals, color) {
    if (!el) return;
    const w = +el.getAttribute('width'), h = +el.getAttribute('height');
    el.innerHTML = `<path d="${linePath(vals, w, h, 2)}" fill="none" stroke="${color}" stroke-width="1.2"/>`;
  }

  /* offset 带。scale 是本视图共用的刻度（最大落后量），传同一个值才能横向比较。
     compact=true 时省略下方的位点/落后文字，用于表格内嵌套的队列级明细。 */
  function ribbon(cursor, head, fly, scale, compact) {
    const gap = head - cursor;
    const gapPct = gap / Math.max(scale || 1, 1) * 100;
    const flyPct = gap ? fly / gap * gapPct : 0;
    const sev = gap > 500 ? 'rib-warn' : 'rib-ok';
    return `<div class="ribbon">
      <span class="rib-fly" style="width:${flyPct.toFixed(2)}%"></span>
      <span class="${sev}" style="width:${(gapPct - flyPct).toFixed(2)}%"></span>
      <span class="cursor"></span>
      ${gap === 0 && !compact ? '<span class="caught">已追平</span>' : ''}
    </div>${compact ? '' : `<div class="prog"><span>位点 ${fmt(cursor)}</span><span>落后 ${fmt(gap)}</span></div>`}`;
  }

  // 行状态：有死信=异常，落后超阈值=关注，否则正常
  function markOf(c) {
    if (c.dlq > 0) return 'm-bad';
    if (c.head - c.cursor > 500) return 'm-warn';
    return 'm-ok';
  }

  const lagOf = c => c.head - c.cursor;
  const maxLag = list => Math.max(...list.map(lagOf), 1);
  const byGroup = g => consumption.filter(c => c.group === g);
  const byTopic = t => consumption.filter(c => c.topic === t);
  const topic = n => topics.find(t => t.name === n);
  const group = n => groups.find(g => g.name === n);

  // 表单反馈：原型不连后端，统一在页面顶部显示一条「已执行（mock）」
  function notify(text, kind) {
    let bar = document.getElementById('sq-notice');
    if (!bar) {
      bar = document.createElement('div');
      bar.id = 'sq-notice';
      const host = document.querySelector('.content');
      host.insertBefore(bar, host.firstChild);
    }
    bar.className = `notice ${kind || 'ok'}`;
    bar.innerHTML = text;
    bar.scrollIntoView({ block: 'nearest' });
  }

  /* ---------------- 主题 ---------------- */

  // file:// 下 localStorage 可能被浏览器按安全策略拒绝，读写都要兜住，
  // 失败时退化为「本次会话内有效」，不能让整个脚本挂掉
  function readTheme() {
    try { return localStorage.getItem('sq-theme'); } catch (e) { return null; }
  }
  function writeTheme(v) {
    try { localStorage.setItem('sq-theme', v); } catch (e) { /* 忽略：不影响当前页生效 */ }
  }

  // 本脚本在 <head> 里同步执行，此时 body 尚未渲染，
  // 在这里就把主题打到 <html> 上可以避免切换主题后刷新出现白闪
  function applyTheme(v) {
    document.documentElement.dataset.theme = v === 'dark' ? 'dark' : 'light';
  }
  applyTheme(readTheme() || 'light');

  function toggleTheme() {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    writeTheme(next);
    syncToggle();
  }

  function syncToggle() {
    const btn = document.querySelector('.theme-toggle');
    if (!btn) return;
    const dark = document.documentElement.dataset.theme === 'dark';
    // 用汉字而不是 ☀/☾ 符号：符号在部分系统字体里退化成方框或星号，
    // 汉字在任何中文环境下都稳定可读。按钮上写的是「点了会变成什么」
    btn.textContent = dark ? '明色' : '暗色';
    btn.title = dark ? '切换到明色主题' : '切换到暗色主题';
    btn.setAttribute('aria-label', btn.title);
  }

  // 十个页面各自复制了一份顶部条，主题按钮由脚本统一插入，
  // 避免同一段标记散落到每个文件里、改一次要改十处
  document.addEventListener('DOMContentLoaded', function () {
    const bar = document.querySelector('.topbar');
    if (!bar || bar.querySelector('.theme-toggle')) return;
    const btn = document.createElement('button');
    btn.className = 'btn theme-toggle';
    btn.addEventListener('click', toggleTheme);
    const logout = bar.querySelector('a[href$="login.html"]');
    bar.insertBefore(btn, logout || null);
    syncToggle();
  });

  return { NOW, topics, groups, consumption, messages, dlq, delay, transactions, overview,
           fmt, ago, until, time, dur, wave, linePath, spark, ribbon, markOf,
           lagOf, maxLag, byGroup, byTopic, topic, group, notify, toggleTheme };
})();
