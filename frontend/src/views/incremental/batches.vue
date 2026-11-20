<template>
  <section class="page" data-module="incremental-batches">
    <header class="page-head">
      <div>
        <h2>增量批次包</h2>
        <p class="page-desc">
          把指定游标区间的源端变更切成分片并打包导出；索引逐片列出起止游标与记录数，
          只有分片齐备、索引已发布的包才能下载和回放。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/incremental">返回增量采集</RouterLink>
        <button class="btn" type="button" @click="reload">刷新列表</button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="createPackage">
      <label class="filter-item">
        <span>来源标识</span>
        <select v-model="form.source">
          <option v-for="item in sourceOptions" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>起始游标</span>
        <input v-model="form.start" type="number" placeholder="含" required />
      </label>
      <label class="filter-item">
        <span>结束游标</span>
        <input v-model="form.end" type="number" placeholder="含" required />
      </label>
      <label class="filter-item">
        <span>分片条数</span>
        <input v-model="form.shardSize" type="number" placeholder="留空跟随游标批次大小" />
      </label>
      <label class="filter-item">
        <span>游标编号</span>
        <input v-model="form.cursorCode" placeholder="可选，例如 INCR-0004" />
      </label>
      <button class="btn primary" type="submit" :disabled="submitting">
        {{ submitting ? '切分中…' : '切分并打包' }}
      </button>
    </form>

    <p class="hint-text">
      一次切分只使用一个分片口径；切分途中口径被改动会判本次尝试失败，不会被选作回放输入。
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次包编号</th>
          <th>来源</th>
          <th>游标区间</th>
          <th>状态</th>
          <th>已交付尝试</th>
          <th>索引分片</th>
          <th>可回放</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="pkg in packages" :key="pkg.batch_no">
          <td>{{ pkg.batch_no }}</td>
          <td>{{ pkg.source }}</td>
          <td>[{{ pkg.cursor_start }}, {{ pkg.cursor_end }}]</td>
          <td>{{ pkg.status }}</td>
          <td>{{ pkg.delivered_attempt ?? '—' }}</td>
          <td>{{ pkg.index ? `${pkg.index.shard_count} 片 / ${pkg.index.record_count} 条` : '—' }}</td>
          <td>
            <span :class="pkg.deliverable ? 'tag ok' : 'tag warn'">
              {{ pkg.deliverable ? '可回放' : '未交付' }}
            </span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" @click="openDetail(pkg.batch_no)">查看</button>
            <button
              class="link"
              type="button"
              :disabled="!pkg.deliverable"
              @click="download(pkg)"
            >
              下载
            </button>
            <button class="link" type="button" @click="openRetry(pkg)">重试</button>
          </td>
        </tr>
        <tr v-if="!packages.length">
          <td colspan="8" class="empty-state">还没有批次包，先按游标区间切分一个</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 个批次包</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <Teleport to="body">
      <div v-if="detail" class="modal-overlay" @click.self="detail = null">
        <div class="modal-card wide">
          <header class="modal-head">
            <div>
              <h3>批次包 {{ detail.batch_no }}</h3>
              <p class="page-desc">
                来源 {{ detail.source }} · 游标 [{{ detail.cursor_start }}, {{ detail.cursor_end }}] ·
                {{ detail.status }}
              </p>
            </div>
            <button class="btn ghost" type="button" @click="detail = null">&times;</button>
          </header>
          <div class="modal-body">
            <p v-if="detail.index_error" class="error-text">{{ detail.index_error }}</p>

            <h4 class="section-title">索引分片</h4>
            <table v-if="detail.index" class="data-table">
              <thead>
                <tr>
                  <th>分片</th>
                  <th>游标起</th>
                  <th>游标止</th>
                  <th>记录数</th>
                  <th>货架区间</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="shard in detail.index.shards" :key="shard.shard_no">
                  <td>{{ shard.shard_no }}</td>
                  <td>{{ shard.cursor_start }}</td>
                  <td>{{ shard.cursor_end }}</td>
                  <td>{{ shard.record_count }}</td>
                  <td>{{ shard.shelf_range || '—' }}</td>
                </tr>
              </tbody>
            </table>
            <p v-else class="hint-text">没有已发布的索引：本次切分未交付，半成品不能用于回放。</p>

            <h4 class="section-title">切分尝试</h4>
            <table class="data-table">
              <thead>
                <tr>
                  <th>尝试</th>
                  <th>结果</th>
                  <th>切分口径</th>
                  <th>覆盖游标区间</th>
                  <th>实际覆盖段</th>
                  <th>可回放</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="attempt in detail.attempts" :key="attempt.attempt_no">
                  <td>第 {{ attempt.attempt_no }} 次</td>
                  <td>{{ attempt.status }}</td>
                  <td>{{ attempt.切分口径 }}</td>
                  <td>{{ attempt.覆盖游标区间 }}</td>
                  <td>{{ attempt.实际覆盖段 || '—' }}</td>
                  <td>
                    <span :class="attempt.可回放 ? 'tag ok' : 'tag warn'">
                      {{ attempt.可回放 ? '可回放' : '仅核对' }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>

            <h4 class="section-title">覆盖核对</h4>
            <form class="filter-bar" @submit.prevent="runCoverage">
              <label class="filter-item">
                <span>待回放起始游标</span>
                <input v-model="coverageForm.start" type="number" />
              </label>
              <label class="filter-item">
                <span>待回放结束游标</span>
                <input v-model="coverageForm.end" type="number" />
              </label>
              <label class="filter-item">
                <span>核对尝试</span>
                <input v-model="coverageForm.attempt" type="number" placeholder="留空为已交付索引" />
              </label>
              <button class="btn" type="submit">核对相邻分片</button>
            </form>
            <div v-if="coverage" class="result-box">
              <p>
                <span :class="coverage.covered ? 'tag ok' : 'tag warn'">
                  {{ coverage.covered ? '已覆盖' : '未完整覆盖' }}
                </span>
                <span :class="coverage.granularity_consistent ? 'tag ok' : 'tag warn'">
                  {{ coverage.granularity_consistent ? '分片口径一致' : '分片口径不一致' }}
                </span>
                {{ coverage.note }}
              </p>
              <p v-if="coverage.missing_in_range.length" class="error-text">
                缺失游标段：
                <span v-for="segment in coverage.missing_in_range" :key="segment.join('-')">
                  [{{ segment[0] }}, {{ segment[1] }}]
                </span>
              </p>
            </div>

            <h4 class="section-title">整片回放</h4>
            <form class="filter-bar" @submit.prevent="runReplay">
              <label class="filter-item">
                <span>游标位置</span>
                <input v-model="replayForm.cursor" type="number" placeholder="例如 188" />
              </label>
              <label class="filter-item">
                <span>尝试</span>
                <input v-model="replayForm.attempt" type="number" placeholder="留空为已交付索引" />
              </label>
              <button class="btn" type="submit">按整片重放</button>
            </form>
            <div v-if="replay" class="result-box">
              <p>{{ replay.note }}</p>
              <p>本次盘点会重放的货架：{{ replay.shelves.join('、') || '—' }}</p>
            </div>
            <p v-if="replayError" class="error-text">{{ replayError }}</p>

            <h4 class="section-title">采集日志</h4>
            <table class="data-table">
              <thead>
                <tr>
                  <th>日志编号</th>
                  <th>交付状态</th>
                  <th>切分口径</th>
                  <th>覆盖游标区间</th>
                  <th>实际覆盖段</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="log in detail.logs" :key="log.id">
                  <td>{{ log['日志编号'] }}</td>
                  <td>{{ log['交付状态'] }}</td>
                  <td>{{ log['切分口径'] }}</td>
                  <td>{{ log['覆盖游标区间'] }}</td>
                  <td>{{ log['实际覆盖段'] || '—' }}</td>
                </tr>
                <tr v-if="!detail.logs.length">
                  <td colspan="5" class="empty-state">没有关联日志</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="retryTarget" class="modal-overlay" @click.self="retryTarget = null">
        <div class="modal-card">
          <header class="modal-head">
            <h3>重新切分 {{ retryTarget.batch_no }}</h3>
            <button class="btn ghost" type="button" @click="retryTarget = null">&times;</button>
          </header>
          <form class="modal-body" @submit.prevent="submitRetry">
            <p class="hint-text">
              重试会新建一次尝试；上一次的中间片留在原地供采集日志核对，但不会进入本次索引。
            </p>
            <label class="modal-field">
              <span>分片条数</span>
              <input v-model="retryForm.shardSize" type="number" placeholder="例如 6" />
            </label>
            <footer class="modal-foot">
              <button class="btn" type="button" @click="retryTarget = null">取消</button>
              <button class="btn primary" type="submit" :disabled="submitting">重新切分</button>
            </footer>
          </form>
        </div>
      </div>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type Shard = {
  shard_no: number
  cursor_start: number
  cursor_end: number
  record_count: number
  shelf_range: string
}

type Attempt = {
  attempt_no: number
  status: string
  切分口径: string
  覆盖游标区间: string
  实际覆盖段: string
  可回放: boolean
}

type PackageLog = {
  id: number
  日志编号?: string
  交付状态?: string
  切分口径?: string
  覆盖游标区间?: string
  实际覆盖段?: string
}

type Package = {
  batch_no: string
  source: string
  cursor_start: number
  cursor_end: number
  status: string
  delivered_attempt: number | null
  deliverable: boolean
  index_error: string
  index: { shard_count: number; record_count: number; shards: Shard[] } | null
  attempts: Attempt[]
  logs: PackageLog[]
}

type Coverage = {
  covered: boolean
  granularity_consistent: boolean
  note: string
  missing_in_range: number[][]
}

type Replay = { note: string; shelves: string[]; shard_no: number }

const ENDPOINT = '/api/incremental/batches'
const sourceOptions = ref<string[]>(['货架变更', '订单变更', '商品变更'])

const packages = ref<Package[]>([])
const total = ref(0)
const errorMessage = ref('')
const submitting = ref(false)

const form = ref({ source: '货架变更', start: '180', end: '209', shardSize: '', cursorCode: '' })
const retryTarget = ref<Package | null>(null)
const retryForm = ref({ shardSize: '' })

const detail = ref<Package | null>(null)
const coverageForm = ref({ start: '', end: '', attempt: '' })
const coverage = ref<Coverage | null>(null)
const replayForm = ref({ cursor: '', attempt: '' })
const replay = ref<Replay | null>(null)
const replayError = ref('')

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await fetchJson<{ items: Package[]; total: number }>(`${ENDPOINT}?size=100`)
    packages.value = payload.items ?? []
    total.value = payload.total ?? packages.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次包列表读取失败'
  }
}

async function createPackage() {
  errorMessage.value = ''
  submitting.value = true
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({
        values: {
          来源标识: form.value.source,
          起始游标: form.value.start,
          结束游标: form.value.end,
          分片条数: form.value.shardSize || undefined,
          游标编号: form.value.cursorCode || undefined,
        },
      }),
    })
    const payload = await response.json()
    if (!response.ok) {
      throw new Error(payload.detail || payload.message || '切分失败')
    }
    if (!payload.ok) {
      errorMessage.value = payload.message
    }
    await reload()
    if (payload.entry?.batch_no) {
      await openDetail(payload.entry.batch_no)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '切分失败'
  } finally {
    submitting.value = false
  }
}

async function openDetail(batchNo: string) {
  coverage.value = null
  replay.value = null
  replayError.value = ''
  try {
    detail.value = await fetchJson<Package>(`${ENDPOINT}/${batchNo}`)
    coverageForm.value = {
      start: String(detail.value.cursor_start),
      end: String(detail.value.cursor_end),
      attempt: '',
    }
    replayForm.value = { cursor: '', attempt: '' }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次包详情读取失败'
  }
}

function openRetry(pkg: Package) {
  retryTarget.value = pkg
  retryForm.value = { shardSize: '' }
}

async function submitRetry() {
  if (!retryTarget.value) return
  submitting.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${retryTarget.value.batch_no}/retry`, {
      method: 'POST',
      body: JSON.stringify({
        values: { 分片条数: retryForm.value.shardSize || undefined },
      }),
    })
    const payload = await response.json()
    if (!response.ok) {
      throw new Error(payload.detail || payload.message || '重试失败')
    }
    if (!payload.ok) {
      errorMessage.value = payload.message
    }
    retryTarget.value = null
    await reload()
    await openDetail(payload.entry.batch_no)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重试失败'
  } finally {
    submitting.value = false
  }
}

async function download(pkg: Package) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${pkg.batch_no}/download`)
    if (!response.ok) {
      const payload = await response.json()
      throw new Error(payload.detail || '下载失败')
    }
    const blob = await response.blob()
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = `${pkg.batch_no}.zip`
    anchor.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function runCoverage() {
  if (!detail.value) return
  errorMessage.value = ''
  try {
    const query = new URLSearchParams()
    if (coverageForm.value.start) query.set('start', coverageForm.value.start)
    if (coverageForm.value.end) query.set('end', coverageForm.value.end)
    if (coverageForm.value.attempt) query.set('attempt', coverageForm.value.attempt)
    coverage.value = await fetchJson<Coverage>(
      `${ENDPOINT}/${detail.value.batch_no}/coverage?${query.toString()}`,
    )
  } catch (error) {
    coverage.value = null
    errorMessage.value = error instanceof Error ? error.message : '覆盖核对失败'
  }
}

async function runReplay() {
  if (!detail.value || !replayForm.value.cursor) return
  replayError.value = ''
  try {
    const query = new URLSearchParams({ cursor: replayForm.value.cursor })
    if (replayForm.value.attempt) query.set('attempt', replayForm.value.attempt)
    replay.value = await fetchJson<Replay>(
      `${ENDPOINT}/${detail.value.batch_no}/replay?${query.toString()}`,
    )
  } catch (error) {
    replay.value = null
    replayError.value = error instanceof Error ? error.message : '回放失败'
  }
}

onMounted(async () => {
  try {
    const meta = await fetchJson<{ sources: string[] }>(`${ENDPOINT}/sources`)
    if (meta.sources?.length) sourceOptions.value = meta.sources
  } catch {
    /* 来源提示拿不到时保留内置选项 */
  }
  await reload()
})
</script>
