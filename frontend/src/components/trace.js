import {panel} from './common/heading.js';
import {records} from './common/records.js';
import {esc} from '../utils/dom.js';
import {submit} from '../hooks/submit.js';

const STATUS_LABEL = {listed: '在用', withdrawn: '已撤牌', unlisted: '未挂牌', insufficient: '证据不足'};
const EFFECT_LABEL = {put: '挂牌/更新', withdraw: '撤牌', carry: '沿用前批', absent: '本批无牌面'};

function timelineTable(trace) {
  if (!trace.timeline.length) return '<p class="empty">该牌面没有任何到货批次记录。</p>';
  const rows = trace.timeline.map(row=>[
    esc(row.batch_no),
    esc(EFFECT_LABEL[row.effect] || row.effect_label),
    row.content ? esc(row.content) : '<span class="note">—</span>',
    row.source_sequence ? esc(row.source_sequence) : '<span class="note">—</span>',
    row.source_event_id ? esc(row.source_event_id) : '<span class="note">—</span>',
    row.source_receipt ? esc(row.source_receipt) : '<span class="note">—</span>',
    `<span class="status" data-state="${esc(row.status)}">${esc(STATUS_LABEL[row.status] || row.status)}</span>`,
  ]);
  return records(['到货批号', '批末处置', '当时文案', '来源序号', '事件编号', '接收行', '批末状态'], rows);
}

function unverifiedBlock(trace) {
  if (!trace.unverified_events.length) return '';
  const rows = trace.unverified_events.map(row=>[
    esc(row.batch_no), esc(row.receipt), esc(row.sequence), esc(row.event_id), esc(row.reason),
  ]);
  return `<details open><summary>历史未核实信封（${trace.unverified_events.length}）</summary>
    ${records(['到货批号', '接收行', '序号', '事件编号', '依据'], rows)}</details>`;
}

function resultBlock(trace) {
  const current = trace.as_of
    ? `<dl class="metrics"><div><dt>截止批号</dt><dd>${esc(trace.queried_batch || trace.available_batches.at(-1))}</dd></div>
       <div><dt>分区流</dt><dd>${esc(trace.stream)}</dd></div>
       <div><dt>当时文案</dt><dd>${esc(trace.as_of.content)}</dd></div>
       <div><dt>来源序号</dt><dd>${esc(trace.as_of.source_sequence)}</dd></div></dl>`
    : `<p class="note">${esc(trace.note)}</p>`;
  const baseline = trace.baseline
    ? `<p class="note">起始快照基线：${esc(trace.baseline.content)}（来源序号 ${esc(trace.baseline.source_sequence)}）</p>`
    : '';
  return `<div class="trace-result">
      <p><strong>${esc(trace.sign_id)}</strong>
        <span class="status" data-state="${esc(trace.status)}">${esc(trace.status_label)}</span>
        ${trace.matched_streams?.length > 1 ? `<span class="note">匹配多条流，已展示 ${esc(trace.stream)}</span>` : ''}
      </p>
      ${baseline}${current}${unverifiedBlock(trace)}
      <h3>按批次时间线</h3>${timelineTable(trace)}
    </div>`;
}

export function tracePanel(identity, evidence) {
  const batches = evidence.report.sections.find(section=>section.title === '分批推进记录')?.rows || [];
  const options = [...new Set(batches.map(row=>row['到货批号']))].map(value=>`<option value="${esc(value)}">${esc(value)}</option>`).join('');
  return panel('档口牌批次追溯', `
    <form id="trace-form" class="trace-form">
      <label>牌面编号（大小写敏感）<input name="sign_id" maxlength="64" required placeholder="例如 N-07，已撤牌也可追查"></label>
      <label>分区流（可选）<input name="stream" maxlength="64" placeholder="留空则自动匹配"></label>
      <label>截止批号（可选）<select name="batch"><option value="">全部批次</option>${options}</select></label>
      <button>追溯</button>
    </form>
    <div id="trace-output" role="status"><p class="note">只读查询：不重跑执行，已签署发布单与校验码保持不变。</p></div>
  `);
}

export function bindTrace(api, identity, fail) {
  submit('trace-form', async data=>{
    const params = new URLSearchParams();
    params.set('sign_id', data.get('sign_id').trim());
    const stream = data.get('stream').trim();
    if (stream) params.set('stream', stream);
    const batch = data.get('batch');
    if (batch) params.set('batch', batch);
    const output = document.getElementById('trace-output');
    output.innerHTML = '<p class="note">查询中…</p>';
    try {
      const trace = await api(`/evidence/${identity}/trace?${params.toString()}`);
      output.innerHTML = resultBlock(trace);
    } catch (error) {
      output.innerHTML = `<p class="error">${esc(error.message)}</p>`;
    }
  }, fail);
}
