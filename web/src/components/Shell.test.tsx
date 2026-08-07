import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { Shell } from './Shell'
import { api } from '../api/client'
import type { SystemInfo } from '../api/types'

const base: SystemInfo = {
  disk: { total_bytes: 1000, free_bytes: 100, used_percent: 86.3 },
  watermark_percent: 85,
  write_blocked: false,
  data_dir_bytes: 500,
  go_heap_inuse_bytes: 1024,
  go_sys_bytes: 2048,
  goroutines: 10,
  uptime_seconds: 60,
}

function renderShell() {
  return render(
    <MemoryRouter>
      <Shell title="测试页"><p>内容</p></Shell>
    </MemoryRouter>,
  )
}

beforeEach(() => vi.restoreAllMocks())
afterEach(() => vi.restoreAllMocks())

describe('Shell 拒写横幅', () => {
  it('未拒写时不渲染横幅', async () => {
    vi.spyOn(api, 'system').mockResolvedValue(base)
    renderShell()
    await screen.findByText('内容')
    expect(screen.queryByText(/拒写保读/)).toBeNull()
  })

  it('拒写时渲染横幅并带上百分比与水位线', async () => {
    vi.spyOn(api, 'system').mockResolvedValue({ ...base, write_blocked: true })
    renderShell()
    await waitFor(() => expect(screen.getByText(/拒写保读/)).toBeTruthy())
    expect(screen.getByText(/86\.3%/)).toBeTruthy()
    expect(screen.getByText(/水位线 85%/)).toBeTruthy()
  })

  it('取数失败时静默：外壳不该因为一个辅助读数而报错', async () => {
    vi.spyOn(api, 'system').mockRejectedValue(new Error('boom'))
    renderShell()
    await screen.findByText('内容')
    expect(screen.queryByText(/boom/)).toBeNull()
  })
})
