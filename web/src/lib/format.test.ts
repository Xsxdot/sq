import { describe, it, expect } from 'vitest'
import { bytes, uptime } from './format'

describe('bytes', () => {
  it('小于 1KB 时按字节显示', () => {
    expect(bytes(0)).toBe('0 B')
    expect(bytes(999)).toBe('999 B')
  })
  it('逐级进位到合适的单位', () => {
    expect(bytes(1024)).toBe('1.0 KB')
    expect(bytes(1536)).toBe('1.5 KB')
    expect(bytes(1024 * 1024 * 2.5)).toBe('2.5 MB')
    expect(bytes(1024 ** 3 * 3)).toBe('3.0 GB')
  })
  it('三位数以上不再保留小数：诊断读数看量级不看精度', () => {
    expect(bytes(1024 * 128)).toBe('128 KB')
  })
})

describe('uptime', () => {
  it('分钟以下说秒', () => {
    expect(uptime(42)).toBe('42s')
  })
  it('一小时以下说分', () => {
    expect(uptime(600)).toBe('10m')
  })
  it('一天以下说时分', () => {
    expect(uptime(3600 * 5 + 60 * 7)).toBe('5h7m')
  })
  it('一天以上说天时', () => {
    expect(uptime(86400 * 3 + 3600 * 7)).toBe('3d7h')
  })
})
