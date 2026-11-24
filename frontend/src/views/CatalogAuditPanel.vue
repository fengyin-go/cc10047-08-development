<template>
  <section class="cap-panel" data-testid="catalog-audit-panel">
    <header class="cap-head">
      <h3>{{ isAudit ? '湖仓审计待办 · 目录同步确认范围' : '目录同步逐表确认 · 目标版本对账' }}</h3>
      <button class="btn" type="button" data-testid="cap-refresh" @click="refreshAll">刷新复查</button>
    </header>

    <!-- 操作条 -->
    <div class="cap-controls">
      <label class="cap-field">
        <span>同步批次</span>
        <input v-model="syncId" data-testid="cap-sync-id" />
      </label>
      <template v-if="!isAudit">
        <label class="cap-field">
          <span>同步表</span>
          <input v-model="tables" data-testid="cap-tables" />
        </label>
        <label class="cap-field">
          <span>目标版本</span>
          <input v-model="version" data-testid="cap-version" />
        </label>
        <label class="cap-field">
          <span>漏收回执表</span>
          <input v-model="miss" data-testid="cap-miss" />
        </label>
        <button class="btn primary" type="button" data-testid="cap-run-sync" :disabled="busy" @click="runSync">
          运行同步
        </button>
        <button class="btn" type="button" data-testid="cap-retry" :disabled="busy" @click="retryConcurrent">
          并发重试×8
        </button>
      </template>
      <template v-else>
        <button class="btn primary" type="button" data-testid="cap-run-audit" :disabled="busy" @click="runAudit">
          发起审计（读确认记录）
        </button>
      </template>
      <span v-if="message" class="cap-msg" data-testid="cap-message">{{ message }}</span>
    </div>

    <!-- 审计结论摘要 -->
    <div v-if="auditSummary" class="cap-summary" data-testid="cap-audit-summary">
      <span>本轮实查：<b>{{ joinList(auditSummary.checked_tables) }}</b></span>
      <span>待查范围：<b data-testid="cap-pending-scope">{{ joinList(auditSummary.pending_scope) || '无' }}</b></span>
      <span>回执缺失：<b>{{ joinList(auditSummary.missing_receipt_tables) || '无' }}</b></span>
    </div>

    <!-- 逐表确认记录 -->
    <h4 v-if="!isAudit">目录同步确认记录（刷新后可重读）</h4>
    <table v-if="!isAudit" class="cap-table" data-testid="cap-confirmations">
      <thead>
        <tr>
          <th>表名</th><th>目标版本</th><th>回执状态</th><th>有效写入</th>
          <th>重试次数</th><th>原差异行数</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="c in confirmations" :key="String(c.confirmation_id)" :data-table="String(c.table_name)">
          <td>{{ c.table_name }}</td>
          <td>{{ c.target_version }}</td>
          <td :data-receipt="String(c.receipt_status)">
            {{ receiptText(String(c.receipt_status), Boolean(c.late)) }}
          </td>
          <td data-testid="cap-write-count">{{ c.write_count }}</td>
          <td>{{ c.retry_count }}</td>
          <td>{{ c.diff_rows }}</td>
          <td>
            <button
              v-if="!c.confirmed"
              class="link"
              type="button"
              :data-testid="`cap-receipt-${c.table_name}`"
              @click="submitReceipt(String(c.table_name))"
            >
              补传回执
            </button>
            <span v-else class="cap-ok">已确认</span>
          </td>
        </tr>
        <tr v-if="!confirmations.length"><td colspan="7" class="cap-empty">暂无确认记录</td></tr>
      </tbody>
    </table>

    <!-- 实际落地目标记录 -->
    <h4 v-if="!isAudit">目标 Catalog 实际落地记录</h4>
    <table v-if="!isAudit" class="cap-table" data-testid="cap-targets">
      <thead><tr><th>表名</th><th>目标版本</th><th>有效写入次数</th><th>指纹</th><th>落地时间</th></tr></thead>
      <tbody>
        <tr v-for="t in targets" :key="`${t.target_catalog}-${t.table_name}`">
          <td>{{ t.table_name }}</td>
          <td>{{ t.target_version }}</td>
          <td data-testid="cap-target-write-count">{{ t.write_count }}</td>
          <td class="cap-mono">{{ t.fingerprint }}</td>
          <td>{{ formatTime(t.landed_at) }}</td>
        </tr>
        <tr v-if="!targets.length"><td colspan="5" class="cap-empty">暂无落地记录</td></tr>
      </tbody>
    </table>

    <!-- 证据 -->
    <h4 v-if="!isAudit">不一致证据（原差异 / 目标版本 / 迟到回执）</h4>
    <table v-if="!isAudit" class="cap-table" data-testid="cap-evidence">
      <thead><tr><th>证据</th><th>表</th><th>类型</th><th>目标版本</th><th>状态</th><th>迟到回执</th></tr></thead>
      <tbody>
        <tr v-for="e in evidence" :key="String(e.evidence_id)">
          <td>{{ e.evidence_id }}</td>
          <td>{{ e.table_name }}</td>
          <td>{{ e.kind }}</td>
          <td>{{ e.target_version }}</td>
          <td :data-evidence-status="String(e.status)">{{ e.status }}</td>
          <td class="cap-mono">{{ e.late_receipt ? JSON.stringify(e.late_receipt) : '—' }}</td>
        </tr>
        <tr v-if="!evidence.length"><td colspan="6" class="cap-empty">暂无证据</td></tr>
      </tbody>
    </table>

    <!-- 审计待办 -->
    <h4 v-if="isAudit">湖仓审计待办（目录同步完成哪张表即更新哪张表）</h4>
    <table v-if="isAudit" class="cap-table" data-testid="cap-todos">
      <thead><tr><th>表名</th><th>待办状态</th><th>回执状态</th><th>目标版本</th><th>所属批次</th><th>核查时间</th></tr></thead>
      <tbody>
        <tr v-for="t in todos" :key="String(t.table_name)" :data-todo="String(t.status)">
          <td>{{ t.table_name }}</td>
          <td>{{ t.status }}</td>
          <td>{{ t.receipt_status }}</td>
          <td>{{ t.target_version }}</td>
          <td>{{ t.last_sync_id }}</td>
          <td>{{ formatTime(t.checked_at) }}</td>
        </tr>
        <tr v-if="!todos.length"><td colspan="6" class="cap-empty">暂无审计待办</td></tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

const props = defineProps<{ moduleKey: string }>()
const emit = defineEmits<{ (e: 'changed'): void }>()

const isAudit = computed(() => props.moduleKey === 'lake_audit')

type Row = Record<string, unknown>

const syncId = ref('ui-sync-1')
const tables = ref('A,B')
const version = ref('target-v1')
const miss = ref('B')
const busy = ref(false)
const message = ref('')

const confirmations = ref<Row[]>([])
const targets = ref<Row[]>([])
const evidence = ref<Row[]>([])
const todos = ref<Row[]>([])
const auditSummary = ref<Row | null>(null)

function joinList(value: unknown): string {
  return Array.isArray(value) ? value.map((v) => String(v)).join('、') : ''
}

function receiptText(status: string, late: boolean): string {
  if (status === 'missing') return '回执缺失'
  if (status === 'late' || late) return '迟到补确认'
  return '已确认'
}

function formatTime(value: unknown): string {
  if (!value) return '—'
  try {
    return new Date(String(value)).toLocaleTimeString()
  } catch {
    return String(value)
  }
}

async function postJson(path: string, body: Row): Promise<Row> {
  const resp = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const data = (await resp.json()) as Row
  if (!resp.ok || data.ok === false) {
    throw new Error(String(data.message ?? `接口返回 ${resp.status}`))
  }
  return data
}

async function getRows(path: string): Promise<Row[]> {
  const resp = await request(path)
  if (!resp.ok) throw new Error(`接口返回 ${resp.status}`)
  const data = await resp.json()
  return Array.isArray(data) ? (data as Row[]) : ((data.items as Row[]) ?? [])
}

async function refreshCatalog(): Promise<void> {
  const q = `?sync_id=${encodeURIComponent(syncId.value)}`
  const [confs, tgts, evs] = await Promise.all([
    getRows(`/api/catalog-sync/confirmations${q}`),
    getRows('/api/catalog-sync/targets'),
    getRows(`/api/catalog-sync/evidence${q}`),
  ])
  confirmations.value = confs
  targets.value = tgts
  evidence.value = evs
}

async function refreshAudit(): Promise<void> {
  const q = syncId.value ? `?sync_id=${encodeURIComponent(syncId.value)}` : ''
  const [todoRows, scope] = await Promise.all([
    getRows('/api/lake-audit/todos'),
    request(`/api/lake-audit/scope${q}`).then((r) => r.json()) as Promise<Row>,
  ])
  todos.value = todoRows
  if (scope && Array.isArray(scope.pending_scope)) {
    auditSummary.value = {
      checked_tables: scope.pending_audit_tables ?? [],
      pending_scope: scope.pending_scope,
      missing_receipt_tables: scope.missing_receipt_tables ?? [],
    }
  }
}

async function refreshAll(): Promise<void> {
  message.value = ''
  try {
    if (isAudit.value) await refreshAudit()
    else await refreshCatalog()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '刷新失败'
  }
}

async function runSync(): Promise<void> {
  busy.value = true
  message.value = ''
  try {
    const tableList = tables.value.split(/[,，、]/).map((s) => s.trim()).filter(Boolean)
    const missList = miss.value.split(/[,，、]/).map((s) => s.trim()).filter(Boolean)
    const data = await postJson('/api/catalog-sync/run', {
      sync_id: syncId.value,
      tables: tableList,
      target_version: version.value,
      miss_receipt_for: missList,
    })
    message.value = String(data.message ?? '同步完成')
    await refreshCatalog()
    emit('changed')
  } catch (error) {
    message.value = error instanceof Error ? error.message : '同步失败'
  } finally {
    busy.value = false
  }
}

async function retryConcurrent(): Promise<void> {
  busy.value = true
  message.value = ''
  try {
    const tasks = Array.from({ length: 8 }, () =>
      postJson('/api/catalog-sync/retry', { sync_id: syncId.value }).catch(() => null),
    )
    await Promise.all(tasks)
    message.value = '已并发重试 8 次，有效写入应保持每表 1 次'
    await refreshCatalog()
    emit('changed')
  } finally {
    busy.value = false
  }
}

async function submitReceipt(table: string): Promise<void> {
  message.value = ''
  try {
    await postJson('/api/catalog-sync/receipt', {
      sync_id: syncId.value,
      table,
      receipt: { ack_id: `ui-ack-${table}-${Date.now()}`, status: 'confirmed' },
    })
    message.value = `${table} 的迟到回执已补传`
    await refreshCatalog()
    emit('changed')
  } catch (error) {
    message.value = error instanceof Error ? error.message : '回执补传失败'
  }
}

async function runAudit(): Promise<void> {
  busy.value = true
  message.value = ''
  try {
    const data = await postJson('/api/lake-audit/run', {
      entry_id: 1,
      sync_id: syncId.value || undefined,
    })
    auditSummary.value = data
    message.value = String(data.message ?? '审计完成')
    await refreshAudit()
    emit('changed')
  } catch (error) {
    message.value = error instanceof Error ? error.message : '审计失败'
  } finally {
    busy.value = false
  }
}

onMounted(refreshAll)
</script>

<style scoped>
.cap-panel {
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
  margin: 14px 0;
}
.cap-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.cap-head h3 { margin: 0; font-size: 15px; }
.cap-panel h4 { margin: 14px 0 6px; font-size: 13px; color: var(--muted); }
.cap-controls { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 10px; margin: 10px 0; }
.cap-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.cap-field input { border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; min-width: 120px; }
.cap-msg { font-size: 12px; color: var(--brand); }
.cap-summary { display: flex; flex-wrap: wrap; gap: 16px; font-size: 13px; background: #fff; border: 1px dashed var(--border); border-radius: 8px; padding: 8px 12px; }
.cap-table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; font-size: 13px; }
.cap-table th, .cap-table td { border-bottom: 1px solid #eef2f7; padding: 6px 10px; text-align: left; }
.cap-table th { background: #f1f5f9; font-weight: 600; }
.cap-empty { text-align: center; color: var(--muted); padding: 14px; }
.cap-ok { color: #15803d; }
.cap-mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11px; color: var(--muted); }
.link { background: none; border: 0; color: var(--brand); cursor: pointer; padding: 0; }
</style>
