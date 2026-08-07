/**
 * 迷你折线
 *
 * 职责：
 *   - 在读数旁给一条 60×22 的趋势线，回答「这个数是刚变的还是一直这样」
 *
 * 边界：
 *   - 不画坐标轴、不带交互：它是读数的注脚，不是图表
 */
import { linePath } from '../lib/derive'

export interface SparkProps {
  values: number[]
  color: string
  width?: number
  height?: number
}

export function Spark({ values, color, width = 60, height = 22 }: SparkProps) {
  const d = linePath(values, width, height, 2)
  return (
    <svg width={width} height={height} aria-hidden="true">
      {d && <path d={d} fill="none" stroke={color} strokeWidth="1.2" />}
    </svg>
  )
}