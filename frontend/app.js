/**
 * RapidAlert Dashboard — app.js
 * Single-file vanilla JS. WebSocket-driven, no framework.
 */

const App = {
  ws: null,
  wsReady: false,
  cameras: {},      // { name: { config, result, lastTs, thumbB64 } }
  alerts: [],
  metrics: {},
  prompts: { master: '', cameras: {} },
  alertCount: 0,
  activeFilter: 'all',

  // ════════════════════════════════════════════════════════════
  //  Bootstrap
  // ════════════════════════════════════════════════════════════
  init() {
    this.bindUIEvents();
    this.connectWS();
    this.startTimestampTicker();
  },

  // ════════════════════════════════════════════════════════════
  //  WebSocket
  // ════════════════════════════════════════════════════════════
  connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.ws = new WebSocket(`${proto}//${location.host}/ws`);

    this.ws.onopen = () => {
      this.wsReady = true;
      this._wsStatus(true);
    };

    this.ws.onclose = () => {
      this.wsReady = false;
      this._wsStatus(false);
      setTimeout(() => this.connectWS(), 3000);
    };

    this.ws.onerror = () => {
      this.wsReady = false;
      this._wsStatus(false);
    };

    this.ws.onmessage = ({ data }) => {
      try { this.handleMessage(JSON.parse(data)); }
      catch (e) { console.warn('WS parse error:', e); }
    };
  },

  handleMessage(msg) {
    switch (msg.type) {
      case 'init':    return this.onInit(msg);
      case 'result':  return this.onResult(msg);
      case 'alert':   return this.onAlert(msg);
      case 'metrics': return this.onMetrics(msg);
      case 'cameras': return this.onCameras(msg);
      case 'prompts': return this.onPrompts(msg);
      case 'ping':    break; // keep-alive, no-op
    }
  },

  // ════════════════════════════════════════════════════════════
  //  Message handlers
  // ════════════════════════════════════════════════════════════
  onInit(msg) {
    // Build camera registry from config
    this.cameras = {};
    for (const cam of (msg.cameras || [])) {
      this.cameras[cam.name] = { config: cam, result: null, lastTs: 0, thumbB64: null };
      this._renderCard(cam.name);
    }

    // Apply known results
    for (const [name, result] of Object.entries(msg.results || {})) {
      if (this.cameras[name]) {
        this.cameras[name].result = result;
        this.cameras[name].lastTs = result.ts || 0;
        this._applyResult(name, result, null);
      }
    }

    // Seed alerts (oldest → newest in DOM means newest at top)
    const alerts = (msg.alerts || []).slice().reverse();
    for (const alert of alerts) this._renderAlert(alert, false);
    this._syncAlertCount();

    // Metrics
    if (msg.metrics) this._applyMetrics(msg.metrics);

    // Prompts
    if (msg.prompts) this._applyPrompts(msg.prompts);

    // Fetch initial thumbnails from REST
    this._fetchAllThumbs();

    this._updateCamCount();
    this._updateCamSelect();
    this._syncEmptyState();
  },

  onResult(msg) {
    const { cam, result, latency, thumbnail_b64 } = msg;

    if (!this.cameras[cam]) {
      this.cameras[cam] = { config: { name: cam }, result: null, lastTs: 0, thumbB64: null };
      this._renderCard(cam);
      this._syncEmptyState();
    }

    this.cameras[cam].result = result;
    this.cameras[cam].lastTs = result.ts || Date.now() / 1000;
    if (thumbnail_b64) this.cameras[cam].thumbB64 = thumbnail_b64;

    this._applyResult(cam, result, thumbnail_b64);
    this._updateCamCount();
  },

  onAlert(msg) {
    const alert = msg.data;
    this.alerts.unshift(alert);
    this._renderAlert(alert, true);
    this._syncAlertCount();
  },

  onMetrics(msg) {
    this._applyMetrics(msg.data || msg);
  },

  onCameras(msg) {
    const cams = msg.data || [];
    const names = new Set(cams.map(c => c.name));

    // Remove stale cards
    for (const name of Object.keys(this.cameras)) {
      if (!names.has(name)) {
        delete this.cameras[name];
        document.getElementById(`card-${this._eid(name)}`)?.remove();
      }
    }

    // Add / update
    for (const cam of cams) {
      if (!this.cameras[cam.name]) {
        this.cameras[cam.name] = { config: cam, result: null, lastTs: 0, thumbB64: null };
        this._renderCard(cam.name);
      } else {
        this.cameras[cam.name].config = cam;
      }
    }

    this._updateCamCount();
    this._updateCamSelect();
    this._syncEmptyState();
    this._syncCamTable(cams);
  },

  onPrompts(msg) {
    this._applyPrompts(msg.data || msg);
  },

  // ════════════════════════════════════════════════════════════
  //  Camera Cards
  // ════════════════════════════════════════════════════════════
  _renderCard(name) {
    const grid = document.getElementById('cameras-grid');
    if (!grid) return;

    // Remove stale
    document.getElementById(`card-${this._eid(name)}`)?.remove();

    const card = document.createElement('div');
    card.className = 'cam-card';
    card.id = `card-${this._eid(name)}`;
    card.dataset.cam = name;
    card.innerHTML = `
      <div class="cam-card-header">
        <span class="cam-card-name">${this._esc(name)}</span>
        <span class="cam-live-dot" id="dot-${this._eid(name)}"></span>
      </div>
      <div class="cam-thumb-wrap">
        <img class="cam-thumb" id="thumb-${this._eid(name)}" src="" alt="${this._esc(name)}" style="display:none">
        <div class="cam-thumb-placeholder" id="ph-${this._eid(name)}">
          <span class="cam-thumb-icon">📷</span>
          <span>Connecting...</span>
        </div>
      </div>
      <div class="cam-badges" id="badges-${this._eid(name)}">
        <span class="badge badge-muted">⚡ —</span>
        <span class="badge badge-muted">🛡 —</span>
        <span class="badge badge-muted">⚠ —</span>
      </div>
      <p class="cam-obs" id="obs-${this._eid(name)}">Awaiting analysis…</p>
      <div class="cam-card-footer">
        <span class="cam-latency" id="lat-${this._eid(name)}"></span>
        <span class="cam-ts"     id="ts-${this._eid(name)}">—</span>
      </div>
    `;
    card.addEventListener('click', () => this._openModal(name));
    grid.appendChild(card);
    this._applyFilter(card, name);
  },

  _applyResult(name, result, thumbB64) {
    const eid = this._eid(name);
    const card = document.getElementById(`card-${eid}`);
    if (!card) return;

    const sev = (result.severity || 'LOW').toLowerCase();
    const saf = (result.safety   || 'UNKNOWN').toUpperCase();
    const act = (result.activity || 'UNKNOWN').toUpperCase();

    // Severity class on card
    card.className = `cam-card sev-${sev}`;

    // Thumbnail
    if (thumbB64) {
      const img = document.getElementById(`thumb-${eid}`);
      const ph  = document.getElementById(`ph-${eid}`);
      if (img) {
        const newSrc = `data:image/jpeg;base64,${thumbB64}`;
        if (img.src !== newSrc) {
          const tempImg = new Image();
          tempImg.onload = () => {
            img.src = newSrc;
            img.style.display = 'block';
            if (ph) ph.style.display = 'none';
          };
          tempImg.src = newSrc;
        }
      }
    }

    // Badges
    const badgesEl = document.getElementById(`badges-${eid}`);
    if (badgesEl) {
      const actCls = { ACTIVE: 'green', IDLE: 'amber', UNKNOWN: 'muted' }[act] || 'muted';
      const safCls = { OK: 'green', WARNING: 'amber', DANGER: 'red', UNKNOWN: 'muted' }[saf] || 'muted';
      const sevCls = { LOW: 'green', MEDIUM: 'amber', HIGH: 'red' }[sev.toUpperCase()] || 'muted';
      badgesEl.innerHTML = `
        <span class="badge badge-${actCls}">⚡ ${act}</span>
        <span class="badge badge-${safCls}">🛡 ${saf}</span>
        <span class="badge badge-${sevCls}">⚠ ${sev.toUpperCase()}</span>
        ${result.workers && result.workers !== '0' ? `<span class="badge badge-neutral">👷 ${this._esc(result.workers)}</span>` : ''}
      `;
    }

    // Observation
    const obsEl = document.getElementById(`obs-${eid}`);
    if (obsEl) obsEl.textContent = result.observation || '';

    // Latency
    const latEl = document.getElementById(`lat-${eid}`);
    if (latEl) latEl.textContent = result.latency ? `${result.latency}s` : '';

    // Live dot
    const dotEl = document.getElementById(`dot-${eid}`);
    if (dotEl) dotEl.className = 'cam-live-dot live-ok';

    // Pulse animation
    card.classList.add('pulse-new');
    setTimeout(() => card.classList.remove('pulse-new'), 1200);

    // Re-apply filter visibility
    this._applyFilter(card, name);
  },

  _applyFilter(card, name) {
    const f = this.activeFilter;
    if (f === 'all') { card.style.display = ''; return; }
    const cam = this.cameras[name];
    const sev = cam?.result?.severity?.toUpperCase() || 'LOW';
    const act = cam?.result?.activity?.toUpperCase() || 'UNKNOWN';
    let show = false;
    if (f === 'high'   && sev === 'HIGH')   show = true;
    if (f === 'medium' && sev === 'MEDIUM')  show = true;
    if (f === 'active' && act === 'ACTIVE')  show = true;
    card.style.display = show ? '' : 'none';
  },

  // ════════════════════════════════════════════════════════════
  //  Alerts
  // ════════════════════════════════════════════════════════════
  _renderAlert(alert, animate) {
    const feed = document.getElementById('alerts-feed');
    if (!feed) return;

    const empty = document.getElementById('alerts-empty');
    if (empty) empty.style.display = 'none';

    const sev = (alert.severity || 'LOW').toLowerCase();
    const ts  = alert.ts ? new Date(alert.ts * 1000).toLocaleTimeString() : '—';
    const sevCls = { low: 'green', medium: 'amber', high: 'red' }[sev] || 'muted';

    const item = document.createElement('div');
    item.className = `alert-item sev-${sev}${animate ? ' alert-enter' : ''}`;
    item.style.cursor = 'pointer';
    item.title = 'Click to expand';
    item.innerHTML = `
      <div class="alert-header">
        <span class="alert-cam">${this._esc(alert.cam)}</span>
        <span class="badge badge-${sevCls} badge-sm">⚠ ${sev.toUpperCase()}</span>
        <span class="alert-ts">${ts}</span>
      </div>
      ${alert.thumbnail_b64 ? `<img class="alert-thumb" src="data:image/jpeg;base64,${alert.thumbnail_b64}" alt="">` : ''}
      <p class="alert-obs">${this._esc((alert.observation || '').slice(0, 120))}${(alert.observation || '').length > 120 ? '…' : ''}</p>
    `;

    item.addEventListener('click', () => this.openAlertModal(alert));

    if (animate && sev === 'high') {
      item.classList.add('alert-shake');
      setTimeout(() => item.classList.remove('alert-shake'), 800);
    }

    feed.insertBefore(item, feed.firstChild);

    // Cap DOM at 60 items
    while (feed.children.length > 60) feed.removeChild(feed.lastChild);

    this.alertCount++;
    this._syncAlertCount();
  },

  openAlertModal(alert) {
    const sev    = (alert.severity || 'LOW').toLowerCase();
    const safety = (alert.safety  || '').toLowerCase();
    const ts     = alert.ts ? new Date(alert.ts * 1000).toLocaleString() : '—';
    const sevCls = { low: 'green', medium: 'amber', high: 'red' }[sev] || 'muted';
    const safeCls = { danger: 'red', warning: 'amber', ok: 'green' }[safety] || 'muted';

    document.getElementById('alert-modal-cam').textContent = alert.cam || 'Alert';
    document.getElementById('alert-modal-ts').textContent = ts;
    document.getElementById('alert-modal-obs').textContent = alert.observation || '—';

    // Badges
    const badges = document.getElementById('alert-modal-badges');
    badges.innerHTML = [
      `<span class="badge badge-${sevCls}">⚠ ${sev.toUpperCase()}</span>`,
      alert.safety ? `<span class="badge badge-${safeCls}">🛡 ${alert.safety}</span>` : '',
      alert.activity ? `<span class="badge badge-muted">${this._esc(alert.activity)}</span>` : '',
    ].join('');

    // Meta
    const meta = document.getElementById('alert-modal-meta');
    const rows = [
      ['Workers', alert.workers],
      ['Machinery', alert.machinery],
    ].filter(([, v]) => v && v !== 'None' && v !== '0');
    meta.innerHTML = rows.map(([k, v]) => `<div><strong>${k}:</strong> ${this._esc(String(v))}</div>`).join('');

    // Thumbnail
    const img   = document.getElementById('alert-modal-img');
    const noThumb = document.getElementById('alert-modal-no-thumb');
    if (alert.thumbnail_b64) {
      img.src = `data:image/jpeg;base64,${alert.thumbnail_b64}`;
      img.style.display = 'block';
      noThumb.style.display = 'none';
    } else {
      img.style.display = 'none';
      noThumb.style.display = 'flex';
    }

    document.getElementById('alert-modal-backdrop').hidden = false;
    document.getElementById('alert-modal').hidden = false;
  },

  closeAlertModal() {
    document.getElementById('alert-modal-backdrop').hidden = true;
    document.getElementById('alert-modal').hidden = true;
  },

  _syncAlertCount() {
    const badge = document.getElementById('alert-count');
    if (badge) badge.textContent = this.alertCount;
  },

  // ════════════════════════════════════════════════════════════
  //  Metrics
  // ════════════════════════════════════════════════════════════
  _applyMetrics(m) {
    this.metrics = m;
    this._setText('metric-aps',   m.analyses_per_sec != null ? m.analyses_per_sec.toFixed(1) : '—');
    this._setText('metric-p95',   m.p95_latency  != null ? `${m.p95_latency}s` : '—');
    this._setText('metric-conc',  m.concurrency  ?? '—');
    this._setText('metric-queue', m.queue_depth  ?? '0');

    // System tab
    this._setText('sys-workers', m.concurrency  ?? '—');
    this._setText('sys-total',   m.total_analyses ?? '—');
    this._setText('sys-p50',     m.p50_latency != null ? `${m.p50_latency}s` : '—');
    this._setText('sys-p95',     m.p95_latency != null ? `${m.p95_latency}s` : '—');
    this._setText('sys-aps',     m.analyses_per_sec != null ? `${m.analyses_per_sec.toFixed(1)}/s` : '—');
  },

  // ════════════════════════════════════════════════════════════
  //  Prompts
  // ════════════════════════════════════════════════════════════
  _applyPrompts(p) {
    this.prompts = p;
    const masterTA = document.getElementById('master-prompt-ta');
    if (masterTA && masterTA !== document.activeElement) masterTA.value = p.master || '';
    this._updateCamSelect();
  },

  _updateCamSelect() {
    const sel = document.getElementById('cam-prompt-sel');
    if (!sel) return;
    const cur = sel.value;
    sel.innerHTML = '<option value="">— Select camera —</option>';
    for (const name of Object.keys(this.cameras)) {
      const opt = document.createElement('option');
      opt.value = name;
      const hasOverride = !!(this.prompts.cameras?.[name]);
      opt.textContent = name + (hasOverride ? ' ✎' : '');
      sel.appendChild(opt);
    }
    if (cur) sel.value = cur;
  },

  // ════════════════════════════════════════════════════════════
  //  Modal
  // ════════════════════════════════════════════════════════════
  _openModal(name) {
    const cam = this.cameras[name];
    if (!cam) return;

    document.getElementById('modal-cam-name').textContent = name;

    const ts = cam.lastTs ? new Date(cam.lastTs * 1000).toLocaleTimeString() : '—';
    document.getElementById('modal-cam-ts').textContent = `Last analysis: ${ts}`;

    // Load full-res frame via REST
    const img  = document.getElementById('modal-frame');
    const loading = document.getElementById('modal-frame-loading');
    img.style.display = 'none';
    loading.style.display = 'flex';
    fetch(`/api/cameras/${encodeURIComponent(name)}/frame?width=1280`)
      .then(r => r.ok ? r.blob() : null)
      .then(blob => {
        if (!blob) return;
        img.src = URL.createObjectURL(blob);
        img.style.display = 'block';
        loading.style.display = 'none';
      })
      .catch(() => { loading.textContent = 'Frame unavailable'; });

    // Analysis
    const result = cam.result;
    const badgesEl = document.getElementById('modal-badges');
    const obsEl    = document.getElementById('modal-obs');
    const metaEl   = document.getElementById('modal-meta');

    if (result) {
      const sev = (result.severity || 'LOW').toUpperCase();
      const saf = (result.safety   || 'UNKNOWN').toUpperCase();
      const act = (result.activity || 'UNKNOWN').toUpperCase();
      const actCls = { ACTIVE: 'green', IDLE: 'amber', UNKNOWN: 'muted' }[act] || 'muted';
      const safCls = { OK: 'green', WARNING: 'amber', DANGER: 'red', UNKNOWN: 'muted' }[saf] || 'muted';
      const sevCls = { LOW: 'green', MEDIUM: 'amber', HIGH: 'red' }[sev] || 'muted';
      badgesEl.innerHTML = `
        <span class="badge badge-${actCls}">⚡ ${act}</span>
        <span class="badge badge-neutral">👷 ${this._esc(result.workers || '0')} workers</span>
        <span class="badge badge-${safCls}">🛡 ${saf}</span>
        <span class="badge badge-${sevCls}">⚠ ${sev}</span>
      `;
      obsEl.textContent = result.observation || '—';
      metaEl.innerHTML = `
        <span>Latency: ${result.latency ?? '—'}s</span>
        ${result.machinery && result.machinery !== 'None' ? `<span>Machinery: ${this._esc(result.machinery)}</span>` : ''}
        <span>At: ${result.ts ? new Date(result.ts * 1000).toLocaleString() : '—'}</span>
      `;
    } else {
      badgesEl.innerHTML = '';
      obsEl.textContent = 'No analysis yet';
      metaEl.innerHTML = '';
    }

    document.getElementById('modal-backdrop').removeAttribute('hidden');
    document.getElementById('cam-modal').removeAttribute('hidden');
  },

  _closeModal() {
    document.getElementById('modal-backdrop').setAttribute('hidden', '');
    document.getElementById('cam-modal').setAttribute('hidden', '');
    const img = document.getElementById('modal-frame');
    if (img.src.startsWith('blob:')) URL.revokeObjectURL(img.src);
    img.src = '';
  },

  // ════════════════════════════════════════════════════════════
  //  Settings Panel
  // ════════════════════════════════════════════════════════════
  _openSettings() {
    document.getElementById('settings-backdrop').removeAttribute('hidden');
    document.getElementById('settings-panel').removeAttribute('hidden');
    // Sync cam table
    this._syncCamTable(Object.values(this.cameras).map(c => c.config));
    // Sync master prompt
    const masterTA = document.getElementById('master-prompt-ta');
    if (masterTA) masterTA.value = this.prompts.master || '';
    // Sync VLM info
    this._fetchVLMEndpoints();
  },

  _closeSettings() {
    document.getElementById('settings-backdrop').setAttribute('hidden', '');
    document.getElementById('settings-panel').setAttribute('hidden', '');
  },

  _syncCamTable(cams) {
    const tbody = document.getElementById('cam-table-body');
    if (!tbody) return;
    tbody.innerHTML = '';
    for (const cam of cams) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><strong>${this._esc(cam.name)}</strong></td>
        <td><span class="url-truncate" title="${this._esc(cam.url || '')}">${this._esc((cam.url || '').slice(0, 35))}…</span></td>
        <td>
          <label class="toggle">
            <input type="checkbox" ${cam.enabled !== false ? 'checked' : ''}
                   data-cam="${this._esc(cam.name)}" class="toggle-enabled">
            <span class="toggle-slider"></span>
          </label>
        </td>
        <td><input type="text" class="input ctx-day" data-cam="${this._esc(cam.name)}"
                   value="${this._esc(cam.normal_context_day || '')}" placeholder="Day context…"></td>
        <td><input type="text" class="input ctx-night" data-cam="${this._esc(cam.name)}"
                   value="${this._esc(cam.normal_context_night || '')}" placeholder="Night context…"></td>
        <td><button class="btn-danger btn-del" data-cam="${this._esc(cam.name)}">🗑</button></td>
      `;
      tbody.appendChild(tr);
    }

    // Bind inline events (delegated from tbody)
    tbody.addEventListener('change', async e => {
      const cam  = e.target.dataset.cam;
      if (!cam) return;
      const cfg  = { ...(this.cameras[cam]?.config || { name: cam, url: '' }) };
      if (e.target.classList.contains('toggle-enabled')) cfg.enabled = e.target.checked;
      if (e.target.classList.contains('ctx-day'))        cfg.normal_context_day   = e.target.value;
      if (e.target.classList.contains('ctx-night'))      cfg.normal_context_night = e.target.value;
      await this._apiUpsertCamera(cfg);
    }, { once: true }); // rebinds each time table redraws

    tbody.addEventListener('click', async e => {
      const btn = e.target.closest('.btn-del');
      if (!btn) return;
      const cam = btn.dataset.cam;
      if (cam && confirm(`Remove camera "${cam}"?`)) await this._apiDeleteCamera(cam);
    }, { once: true });
  },

  // ════════════════════════════════════════════════════════════
  //  API Calls
  // ════════════════════════════════════════════════════════════
  async _apiUpsertCamera(cam) {
    await fetch('/api/cameras', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(cam),
    });
  },

  async _apiDeleteCamera(name) {
    await fetch(`/api/cameras/${encodeURIComponent(name)}`, { method: 'DELETE' });
  },

  async _saveMasterPrompt() {
    const text = document.getElementById('master-prompt-ta')?.value;
    if (text == null) return;
    await fetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ master: text }),
    });
    this._flashSaveFeedback('master-save-fb', '✓ Saved');
  },

  async _saveCamPrompt() {
    const name = document.getElementById('cam-prompt-sel')?.value;
    const text = document.getElementById('cam-prompt-ta')?.value;
    if (!name) return;
    await fetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ cam_name: name, cam_prompt: text || null }),
    });
    this._flashSaveFeedback('cam-save-fb', '✓ Saved');
  },

  async _clearCamPrompt() {
    const name = document.getElementById('cam-prompt-sel')?.value;
    if (!name) return;
    await fetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ cam_name: name, cam_prompt: null }),
    });
    const ta = document.getElementById('cam-prompt-ta');
    if (ta) ta.value = '';
    this._flashSaveFeedback('cam-save-fb', '✓ Cleared');
  },

  async _addCamera() {
    const name = document.getElementById('new-cam-name')?.value.trim();
    const url  = document.getElementById('new-cam-url')?.value.trim();
    if (!name || !url) { this._toast('Enter a name and RTSP URL'); return; }
    await this._apiUpsertCamera({ name, url, enabled: true });
    document.getElementById('new-cam-name').value = '';
    document.getElementById('new-cam-url').value  = '';
    this._toast(`Camera "${name}" added`);
  },

  async _fetchVLMEndpoints() {
    try {
      const r = await fetch('/api/health/vlm');
      const data = await r.json();
      const container = document.getElementById('vlm-health-items');
      const sysEpEl   = document.getElementById('sys-endpoints');
      if (sysEpEl) sysEpEl.textContent = Object.keys(data).length;
      if (container) {
        container.innerHTML = Object.entries(data).map(([url, ok]) => `
          <div class="vlm-health-item vlm-health-${ok ? 'ok' : 'err'}">
            <span class="vlm-url">${this._esc(url)}</span>
            <span class="vlm-health-status">${ok ? '✓ Online' : '✗ Offline'}</span>
          </div>
        `).join('');
      }
    } catch { /* ignore */ }
  },

  // ════════════════════════════════════════════════════════════
  //  Thumbnails (initial REST fetch)
  // ════════════════════════════════════════════════════════════
  _fetchAllThumbs() {
    for (const name of Object.keys(this.cameras)) {
      const eid = this._eid(name);
      const img = document.getElementById(`thumb-${eid}`);
      if (!img || img.src) continue;
      fetch(`/api/cameras/${encodeURIComponent(name)}/frame?width=320&quality=70`)
        .then(r => r.ok ? r.blob() : null)
        .then(blob => {
          if (!blob) return;
          img.src = URL.createObjectURL(blob);
          img.style.display = 'block';
          const ph = document.getElementById(`ph-${eid}`);
          if (ph) ph.style.display = 'none';
        })
        .catch(() => {});
    }
  },

  // ════════════════════════════════════════════════════════════
  //  Timestamp ticker
  // ════════════════════════════════════════════════════════════
  startTimestampTicker() {
    setInterval(() => {
      const now = Date.now() / 1000;
      for (const [name, cam] of Object.entries(this.cameras)) {
        if (!cam.lastTs) continue;
        const el = document.getElementById(`ts-${this._eid(name)}`);
        if (!el) continue;
        const ago = now - cam.lastTs;
        el.textContent = ago < 60
          ? `${Math.round(ago)}s ago`
          : ago < 3600
            ? `${Math.round(ago / 60)}m ago`
            : `${Math.round(ago / 3600)}h ago`;

        // Live dot colour
        const dot = document.getElementById(`dot-${this._eid(name)}`);
        if (dot) {
          if      (ago < 20)  dot.className = 'cam-live-dot live-ok';
          else if (ago < 60)  dot.className = 'cam-live-dot live-warn';
          else                dot.className = 'cam-live-dot';
        }
      }
    }, 1000);
  },

  // ════════════════════════════════════════════════════════════
  //  UI helpers
  // ════════════════════════════════════════════════════════════
  _wsStatus(ok) {
    const dot   = document.getElementById('ws-dot');
    const label = document.getElementById('ws-status');
    if (dot)   dot.className   = `ws-dot ${ok ? 'ws-ok' : 'ws-err'}`;
    if (label) label.textContent = ok ? 'Live' : 'Reconnecting…';
  },

  _updateCamCount() {
    this._setText('metric-cams', Object.keys(this.cameras).length);
  },

  _syncEmptyState() {
    const empty = document.getElementById('empty-cameras');
    if (!empty) return;
    empty.style.display = Object.keys(this.cameras).length === 0 ? '' : 'none';
  },

  _flashSaveFeedback(id, msg) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = msg;
    el.style.animation = 'none';
    void el.offsetWidth;
    el.style.animation = 'fade-out 2s forwards';
  },

  _toast(msg) {
    const c = document.getElementById('toast-container');
    if (!c) return;
    const t = document.createElement('div');
    t.className = 'toast';
    t.textContent = msg;
    c.appendChild(t);
    requestAnimationFrame(() => {
      t.classList.add('show');
      setTimeout(() => {
        t.classList.remove('show');
        setTimeout(() => t.remove(), 300);
      }, 2500);
    });
  },

  _setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  },

  _esc(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  },

  // CSS ID-safe encoding for camera names
  _eid(name) {
    return name.replace(/[^a-zA-Z0-9_-]/g, '_');
  },

  // ════════════════════════════════════════════════════════════
  //  RTSP Scanner
  // ════════════════════════════════════════════════════════════
  async _openScannerTab() {
    // Auto-fill subnet
    const subnetEl = document.getElementById('scan-subnet');
    if (subnetEl && !subnetEl.value) {
      try {
        const r = await fetch('/api/scanner/subnet');
        const d = await r.json();
        if (d.subnet) subnetEl.value = d.subnet;
      } catch { /* ignore */ }
    }
  },

  async _runScan() {
    const btn    = document.getElementById('btn-scan-network');
    const status = document.getElementById('scan-status');
    const subnet = document.getElementById('scan-subnet')?.value.trim() || null;
    const user   = document.getElementById('scan-user')?.value.trim()   || null;
    const pass   = document.getElementById('scan-pass')?.value          || null;

    if (btn) { btn.disabled = true; btn.textContent = '⏳ Scanning…'; }
    if (status) status.textContent = 'Running WS-Discovery + TCP scan…';

    try {
      const r = await fetch('/api/scanner/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          subnet: subnet || null,
          username: user || null,
          password: pass || null,
          ws_timeout: 3.0,
          port_timeout: 0.4,
        }),
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: r.statusText }));
        if (status) status.textContent = `Error: ${err.detail || r.statusText}`;
        return;
      }
      const data = await r.json();
      this._renderScanResults(data.found || []);
      if (status) status.textContent = '';
    } catch (e) {
      if (status) status.textContent = `Network error: ${e.message}`;
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '🔍 Scan Network'; }
    }
  },

  _renderScanResults(found) {
    const wrap  = document.getElementById('scan-results-wrap');
    const count = document.getElementById('scan-count');
    const list  = document.getElementById('scan-results');
    if (!list) return;

    wrap.style.display = '';
    count.textContent  = `${found.length} camera${found.length !== 1 ? 's' : ''} found`;
    list.innerHTML = '';

    if (found.length === 0) {
      list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-3);font-size:0.8rem;">No cameras found. Try a manual subnet or check your network.</div>';
      return;
    }

    for (const cam of found) {
      const methCls = cam.method === 'onvif' ? 'scan-method-onvif' : 'scan-method-tcp';
      // Show first 3 URL guesses; rest toggled
      const urls = cam.rtsp_urls || [];
      const shown = urls.slice(0, 3);
      const urlsHtml = shown.map(u => `
        <div class="scan-url-row">
          <span class="scan-url-text" title="${this._esc(u)}">${this._esc(u)}</span>
          <button class="btn-use-url" data-url="${this._esc(u)}" data-ip="${this._esc(cam.ip)}">+ Use</button>
        </div>
      `).join('');

      const item = document.createElement('div');
      item.className = 'scan-item';
      item.innerHTML = `
        <div class="scan-item-header">
          <span class="scan-ip">${this._esc(cam.ip)}</span>
          <span class="scan-method ${methCls}">${cam.method}</span>
          <span style="font-size:0.68rem;color:var(--text-3);">port ${cam.port}</span>
        </div>
        <div class="scan-urls">${urlsHtml}</div>
      `;
      list.appendChild(item);
    }

    // Click handler for "Use" buttons
    list.querySelectorAll('.btn-use-url').forEach(btn => {
      btn.addEventListener('click', () => {
        const url = btn.dataset.url;
        const ip  = btn.dataset.ip;
        // Auto-fill camera add form and switch to cameras tab
        const nameEl = document.getElementById('new-cam-name');
        const urlEl  = document.getElementById('new-cam-url');
        if (nameEl) nameEl.value = `CAM_${ip.replace(/\./g, '_')}`;
        if (urlEl)  urlEl.value  = url;
        // Switch to cameras tab
        document.querySelectorAll('.stab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.stab-content').forEach(c => c.classList.remove('active'));
        const camTab = document.querySelector('.stab[data-stab="cameras"]');
        if (camTab) camTab.classList.add('active');
        document.getElementById('stab-cameras')?.classList.add('active');
        this._toast(`URL copied → Cameras tab. Adjust name and click Add.`);
      });
    });
  },

  // ════════════════════════════════════════════════════════════
  //  History (persistent SQLite)
  // ════════════════════════════════════════════════════════════
  _histPage: 0,
  _histLimit: 50,

  async _loadHistory() {
    const cam  = document.getElementById('hist-cam-sel')?.value  || '';
    const sev  = document.getElementById('hist-sev-sel')?.value  || '';
    const safe = document.getElementById('hist-safe-sel')?.value || '';
    const offset = this._histPage * this._histLimit;

    const params = new URLSearchParams({ limit: this._histLimit, offset });
    if (cam)  params.set('cam', cam);
    if (sev)  params.set('severity', sev);
    if (safe) params.set('safety', safe);

    try {
      const r = await fetch(`/api/history?${params}`);
      const rows = await r.json();
      this._renderHistoryTable(rows);
      document.getElementById('hist-count').textContent = `${rows.length} rows`;
      document.getElementById('hist-page-label').textContent = `Page ${this._histPage + 1}`;
    } catch (e) {
      this._toast('History load failed: ' + e.message);
    }
  },

  _renderHistoryTable(rows) {
    const tbody = document.getElementById('hist-table-body');
    if (!tbody) return;
    tbody.innerHTML = '';
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-3);padding:20px;">No records</td></tr>';
      return;
    }
    for (const row of rows) {
      const ts = row.ts ? new Date(row.ts * 1000).toLocaleString() : '—';
      const sevCls = { HIGH: 'text-red', MEDIUM: 'text-amber', LOW: 'text-green' }[row.severity] || '';
      const safCls = { DANGER: 'text-red', WARNING: 'text-amber', OK: 'text-green' }[row.safety] || '';
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td style="white-space:nowrap;font-size:0.7rem;font-family:var(--font-mono)">${ts}</td>
        <td style="font-weight:700">${this._esc(row.cam)}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.72rem" title="${this._esc(row.observation||'')}">${this._esc(row.observation||'—')}</td>
        <td>${this._esc(row.activity||'—')}</td>
        <td class="${safCls}">${this._esc(row.safety||'—')}</td>
        <td class="${sevCls}">${this._esc(row.severity||'—')}</td>
        <td style="font-family:var(--font-mono);font-size:0.7rem">${row.latency != null ? row.latency + 's' : '—'}</td>
      `;
      tbody.appendChild(tr);
    }
  },

  async _loadStorageStats() {
    try {
      const r = await fetch('/api/storage/stats');
      const d = await r.json();
      const el = document.getElementById('storage-stats');
      if (!el) return;
      el.innerHTML = [
        ['Total Records', d.total ?? '—'],
        ['Cameras',       d.cameras ?? '—'],
        ['HIGH events',   d.high_count ?? '—'],
        ['DANGER events', d.danger_count ?? '—'],
        ['Avg Latency',   d.avg_latency != null ? `${d.avg_latency.toFixed(2)}s` : '—'],
      ].map(([label, val]) =>
        `<div class="stat-chip"><strong>${val}</strong>${label}</div>`
      ).join('');
    } catch { /* ignore */ }
  },

  _updateHistCamSel() {
    const sel = document.getElementById('hist-cam-sel');
    if (!sel) return;
    const cur = sel.value;
    sel.innerHTML = '<option value="">All cameras</option>';
    for (const name of Object.keys(this.cameras)) {
      const o = document.createElement('option');
      o.value = name; o.textContent = name;
      sel.appendChild(o);
    }
    if (cur) sel.value = cur;
  },

  // ════════════════════════════════════════════════════════════
  //  Event binding
  // ════════════════════════════════════════════════════════════
  bindUIEvents() {
    // Settings open/close
    document.getElementById('btn-settings')?.addEventListener('click', () => this._openSettings());
    document.getElementById('btn-close-settings')?.addEventListener('click', () => this._closeSettings());
    document.getElementById('settings-backdrop')?.addEventListener('click', () => this._closeSettings());

    // Modal close
    document.getElementById('btn-close-modal')?.addEventListener('click', () => this._closeModal());
    document.getElementById('modal-backdrop')?.addEventListener('click', () => this._closeModal());

    // Settings tabs
    document.querySelectorAll('.stab').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.stab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.stab-content').forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(`stab-${btn.dataset.stab}`)?.classList.add('active');
        if (btn.dataset.stab === 'system') this._fetchVLMEndpoints();
        if (btn.dataset.stab === 'history') {
          this._loadHistory();
          this._loadStorageStats();
          this._updateHistCamSel();
        }
        if (btn.dataset.stab === 'scanner') this._openScannerTab();
      });
    });

    // Camera actions
    document.getElementById('btn-add-camera')?.addEventListener('click', () => this._addCamera());
    document.getElementById('new-cam-url')?.addEventListener('keydown', e => {
      if (e.key === 'Enter') this._addCamera();
    });

    // Prompt actions
    document.getElementById('btn-save-master')?.addEventListener('click', () => this._saveMasterPrompt());
    document.getElementById('btn-save-cam-prompt')?.addEventListener('click', () => this._saveCamPrompt());
    document.getElementById('btn-clear-cam-prompt')?.addEventListener('click', () => this._clearCamPrompt());

    // Cam prompt select → load current override
    document.getElementById('cam-prompt-sel')?.addEventListener('change', e => {
      const name = e.target.value;
      const ta   = document.getElementById('cam-prompt-ta');
      if (ta) ta.value = name ? (this.prompts.cameras?.[name] || '') : '';
    });

    // Filter buttons
    document.querySelectorAll('.filter-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeFilter = btn.dataset.filter;
        // Re-apply to all cards
        for (const name of Object.keys(this.cameras)) {
          const card = document.getElementById(`card-${this._eid(name)}`);
          if (card) this._applyFilter(card, name);
        }
      });
    });

    // Scanner actions
    document.getElementById('btn-scan-network')?.addEventListener('click', () => this._runScan());
    document.getElementById('btn-scan-clear')?.addEventListener('click', () => {
      document.getElementById('scan-results-wrap').style.display = 'none';
      document.getElementById('scan-results').innerHTML = '';
      document.getElementById('scan-count').textContent = '0 cameras found';
    });

    // History actions
    document.getElementById('btn-load-history')?.addEventListener('click', () => {
      this._histPage = 0;
      this._loadHistory();
    });
    document.getElementById('btn-hist-prev')?.addEventListener('click', () => {
      if (this._histPage > 0) { this._histPage--; this._loadHistory(); }
    });
    document.getElementById('btn-hist-next')?.addEventListener('click', () => {
      this._histPage++; this._loadHistory();
    });

    // Clear alerts
    document.getElementById('btn-clear-alerts')?.addEventListener('click', () => {
      const feed = document.getElementById('alerts-feed');
      if (feed) feed.innerHTML = `
        <div class="alerts-empty" id="alerts-empty">
          <div class="alerts-empty-icon">✅</div><p>No alerts</p>
        </div>
      `;
      this.alertCount = 0;
      this._syncAlertCount();
      fetch('/api/alerts', { method: 'DELETE' }).catch(() => {});
    });

    // VLM health check button
    document.getElementById('btn-check-vlm')?.addEventListener('click', () => this._fetchVLMEndpoints());

    // Alert modal close
    document.getElementById('btn-close-alert-modal')?.addEventListener('click', () => this.closeAlertModal());
    document.getElementById('alert-modal-backdrop')?.addEventListener('click', () => this.closeAlertModal());

    // Keyboard shortcuts
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') { this.closeAlertModal(); this._closeModal(); this._closeSettings(); }
    });
  },
};

document.addEventListener('DOMContentLoaded', () => App.init());
