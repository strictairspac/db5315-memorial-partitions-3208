import {heading, panel} from '../components/common/heading.js';
import {records} from '../components/common/records.js';
import {batchStateLabels, stationLabels} from '../constants/ingestion.js';
import {topic} from '../constants/project.js';
import {date, esc} from '../utils/dom.js';
import {submit} from '../hooks/submit.js';
import {go} from '../router/hash.js';

function stateBadge(state) {
  return `<span class="status" data-state="${esc(state)}">${esc(batchStateLabels[state] || state)}</span>`;
}

export function ingestionPage(state, identity) {
  const batches = state.ingestionBatches || [];
  const ledger = state.ingestionLedger;
  const replays = state.ingestionReplays || [];
  const isOperator = state.session.role === 'operator';
  const selected = batches.find(row => row.id === identity) || batches[0];

  const batchRows = batches.map(row => [
    `<a href="#ingestion/${row.id}">${esc(stationLabels[row.station] || row.station)} · ${row.segment_count} 段</a>`,
    stateBadge(row.state), esc(row.author), esc(date(row.created_at)),
  ]);

  const createForm = isOperator ? `
    <form id="batch-form" class="inline-form">
      <label>登记点
        <select name="station">
          <option value="east">东登记点</option>
          <option value="west">西登记点</option>
        </select>
      </label>
      <label>分段总数 <input name="segment_count" type="number" min="1" max="1000" value="3" required></label>
      <button>建立汇集批次</button>
    </form>` : '';

  let detail = '<h2>汇集批次</h2><p class="empty">还没有批次。经办员可建立第一个批次，分段补传两个登记点的入园凭条。</p>';
  if (selected) {
    detail = renderDetail(selected, ledger, replays, isOperator);
  }

  return `${heading(topic.modules[4], '乱序可收、重试不重；缺段、冲突、拒收在账簿可查，收齐无冲突才封存并整批回放。')}
    <div class="spread">
      <section aria-label="汇集批次">
        ${records(['批次', '状态', '经办', '建立时间'], batchRows, '尚无汇集批次。')}
        ${createForm}
      </section>
      <aside class="brief">${detail}</aside>
    </div>`;
}

function renderDetail(batch, ledger, replays, isOperator) {
  const id = batch.id;
  const current = ledger && ledger.batch_id === id ? ledger : null;
  const replay = replays.find(row => row.batch_id === id);

  const segmentForm = isOperator && batch.state !== 'sealed' ? `
    <form id="segment-form" class="segment-form">
      <h3>送达一段凭条</h3>
      <label>段序号 <input name="segment_index" type="number" min="1" max="${batch.segment_count}" required></label>
      <label>凭条明细（JSON 数组，字段 slip_id、entered_on）
        <textarea name="slips" rows="6" spellcheck="false" required>[{"slip_id":"SM-0801-01","entered_on":"2026-08-01"}]</textarea>
      </label>
      <button>入库并取回执</button>
    </form>` : '';

  let ledgerBlock = '<p class="note">选择批次后读取账簿。</p>';
  if (current) {
    const conflictRows = Object.entries(current.conflicts).map(([slipId, perDate]) => [
      esc(slipId),
      perDate.map(x => `${esc(x.date)}（出自段 ${x.segments.join('、')}）`).join('<br>'),
    ]);
    const duplicateRows = Object.entries(current.duplicated_ids).map(([slipId, segs]) => [
      esc(slipId), `段 ${segs.join('、')}（同号同日期，封存去重）`]);
    const rejectRows = current.rejected_late_segments.map(row => [
      esc(row.segment_index), esc(row.reason === 'batch_sealed' ? '封存后迟到，已拒收' : row.reason),
      esc(date(row.created_at))]);

    ledgerBlock = `
      <dl class="metrics">
        <div><dt>状态</dt><dd>${stateBadge(current.state)}</dd></div>
        <div><dt>已收段</dt><dd>${current.received_segments.length}/${current.segment_count}</dd></div>
        <div><dt>缺失段</dt><dd>${current.missing_segments.length ? esc(current.missing_segments.join('、')) : '无'}</dd></div>
        <div><dt>凭条行数</dt><dd>${current.row_count}（去重后 ${current.unique_slip_count} 张）</dd></div>
      </dl>
      <p class="note" data-testid="replay-hint">${esc(current.replay_hint)}</p>
      ${panel('缺段与已收段', records(['段序号', '情况'],
        current.missing_segments.map(s => [esc(s), '缺失，等待补传'])
          .concat(current.received_segments.map(s => [esc(s), '已入库'])), '全部段已收齐。'))}
      ${conflictRows.length ? panel('同号日期冲突（不按到达顺序挑选）',
        records(['凭条编号', '各日期出自的段'], conflictRows)) : ''}
      ${duplicateRows.length ? panel('同号同日期跨段重复', records(['凭条编号', '出现位置'], duplicateRows)) : ''}
      ${rejectRows.length ? panel('封存后迟到片段（未混入整批）',
        records(['段序号', '处置', '时间'], rejectRows)) : ''}
      ${current.sealed_digest ? `<details><summary>封存校验码</summary><code class="digest">${esc(current.sealed_digest)}</code></details>` : ''}`;
  }

  const actions = isOperator ? `
    <div class="actions">
      <button type="button" data-action="seal" data-id="${id}" ${batch.state === 'sealed' ? 'disabled' : ''}>封存整批</button>
      <button type="button" data-action="replay" data-id="${id}" ${batch.state !== 'sealed' ? 'disabled' : ''}>整批回放</button>
    </div>` : '';

  const replayBlock = replay ? panel('回放记录', `
    <p>${esc(replay.report.summary)}</p>
    ${records(['凭条编号', '入园日期', '出自段'], replay.report.slips.map(row => [
      esc(row.slip_id), esc(row.entered_on),
      esc((replay.report.source_segments[row.slip_id] || []).join('、'))]))}
    ${replay.report.rejected_late_segments.length
      ? `<p class="note">回放时已排除 ${replay.report.rejected_late_segments.length} 个封存后迟到片段。</p>` : ''}`) : '';

  return `<h2>${esc(stationLabels[batch.station] || batch.station)} · 批次 ${id}</h2>
    ${actions}${ledgerBlock}${segmentForm}${replayBlock}`;
}

export function bindIngestion(state, api, refresh, fail) {
  submit('batch-form', async data => {
    const created = await api('/ingestion/batches', {method: 'POST', data: {
      station: data.get('station'), segment_count: Number(data.get('segment_count'))}});
    go(`ingestion/${created.id}`);
    await refresh();
  }, fail);

  submit('segment-form', async data => {
    const {id} = routeId();
    let slips;
    try {
      slips = JSON.parse(data.get('slips'));
    } catch {
      throw new Error('凭条明细须为 JSON 数组。');
    }
    try {
      const receipt = await api(`/ingestion/batches/${id}/segments`, {method: 'POST', data: {
        segment_index: Number(data.get('segment_index')), slips}});
      state.segmentReceipt = receipt;
    } catch (error) {
      // 封存后迟到会返回 409，刷新以展示拒收留痕
      await refresh();
      throw error;
    }
    await refresh();
  }, fail);

  document.querySelectorAll('[data-action="seal"], [data-action="replay"]').forEach(button => {
    button.addEventListener('click', async () => {
      const id = button.dataset.id;
      try {
        await api(`/ingestion/batches/${id}/${button.dataset.action}`, {method: 'POST'});
        await refresh();
      } catch (error) { fail(error); }
    });
  });
}

function routeId() {
  const [key, id] = (location.hash.slice(1) || '').split('/');
  return {key, id: Number(id) || 0};
}
