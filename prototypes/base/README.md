# sq 控制台原型站

sq 内嵌 Web 控制台（里程碑 M5b）的**形态基准**。零依赖静态页，`file://` 双击 `index.html` 直接打开即可点击浏览。

## 与常规原型站的一点不同

`prototype-site` 的常规用法是扫描真实前端生成镜像。sq 目前**还没有前端**，所以这份 base 是先于真实代码画出来的形态基准：M5b 的 React 控制台照着它开发，开发完成后再用 `prototype-site` 反向重扫一次，让 base 回归「真实前端的镜像」这一常态。

## 页面

| 页面 | 对应功能 | 来源路由 | 消费的 Admin API | 确认状态 |
|------|---------|---------|-----------------|---------|
| index.html | 总览 | `/` | `GET /admin/overview`、`GET /admin/timeseries` | 未确认 |
| pages/topics.html | Topic 列表与新建 | `/topics` | `GET/POST /admin/topics`、`DELETE /admin/topics/{name}` | 未确认 |
| pages/topic-detail.html | Topic 详情与 retention 修改 | `/topics/:name` | `GET/PATCH /admin/topics/{name}` | 未确认 |
| pages/groups.html | 消费组列表 | `/groups` | `GET /admin/groups`、`DELETE /admin/groups/{name}` | 未确认 |
| pages/group-detail.html | 消费组进度与位点重置 | `/groups/:name` | `GET /admin/groups/{name}`、`POST /admin/groups/{name}/reset-cursor` | 未确认 |
| pages/messages.html | 消息查询（Keys / 按队列浏览） | `/messages` | `GET /admin/messages` | 未确认 |
| pages/dlq.html | 死信查看与单条重发 | `/dlq` | `GET /admin/messages`、`POST /admin/dlq/{group}/resend` | 未确认 |
| pages/delay.html | 延时队列视图 | `/delay` | `GET /admin/delay` | 未确认 |
| pages/send.html | 发送测试消息 | `/send` | `POST /admin/messages/send` | 未确认 |
| pages/login.html | 登录 | `/login` | `POST /admin/login` | 未确认 |

## 共享约定

- `shared/app.css`：全站样式与全部可用 class。**页面里不写 `<style>`**，只组合已有 class。
- `shared/mock.js`：全站唯一 mock 数据源，暴露为全局 `SQ`。字段名刻意对齐 Admin API 的返回结构，真实开发时替换数据源即可，渲染逻辑不用改。
- `shared/shell.html`：侧边栏 + 顶部条的外壳样板，供复制。

## 签名元素：offset 带

队列本质是一条 offset 直线，「落后」不是一个数字，而是消费位点到写入头之间那段空隙。带子把这段空隙画出来：斜纹段是已发出未确认的部分，纯色段是尚未发出的部分，空带子表示已追平。

**关键约定：同一视图内所有行共用一把刻度**（该视图的最大落后量），因此带子长短可以直接横向比较。若改成按 `位点 / 写入头` 的比例画，已追平的队列会画成满格、落后一百万条的队列也接近满格，反而看不出差别——这是画这类图最容易犯的错。

## 视觉与交互约定

- 无外部字体、无 CDN、无构建步骤：控制台最终要 `go:embed` 进单二进制，且常部署在无外网的内网机器，任何外部资源都是不能接受的。
- 危险操作（删除 topic / 删除消费组 / 重置位点 / 死信重发）一律走原生 `<dialog>` 二次确认，确认文案必须写清后果与影响范围。
- 语义色：copper 表吞吐、patina 表健康、ochre 表关注、rust 表异常。
