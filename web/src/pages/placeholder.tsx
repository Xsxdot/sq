/**
 * 占位页面组件总集
 *
 * 职责：
 *   - 提供 Task 8–12 真正页面落地前的可编译占位，保证路由表与构建始终可用
 *
 * 边界：
 *   - 每个页面完成时，把 main.tsx 路由表里对应的一行换成真组件并删除这里
 *     的导出；本文件不是常驻结构，是过渡脚手架
 */
function Stub() {
  return <div className="empty"><p>页面待实现</p></div>
}

/** Topic 列表页占位（Task 9 实现） */
export const Topics = Stub
/** 单个 Topic 详情页占位（Task 9 实现） */
export const TopicDetail = Stub
/** 消费组列表页占位（Task 10 实现） */
export const Groups = Stub
/** 单个消费组详情页占位（Task 10 实现） */
export const GroupDetail = Stub
/** 消息查询页占位（Task 11 实现） */
export const Messages = Stub
/** 发送测试消息页占位（Task 11 实现） */
export const Send = Stub
/** 死信队列页占位（Task 12 实现） */
export const Dlq = Stub
/** 延时队列页占位（Task 12 实现） */
export const Delay = Stub