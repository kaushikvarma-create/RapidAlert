/**
 * RapidAlert Dashboard — app.js
 * Single-file vanilla JS. WebSocket-driven, no framework.
 */

const App = {
  ws: null,
  wsReady: false,
  cameras: {},      // { name: { config, results, lastTs, thumbB64, eventPhotos, drift, is_incident } }
  alerts: [],
  metrics: {},
  prompts: { master: '', cameras: {} },
  alertCount: 0,
  activeFilter: 'all',
  activeAlertFilter: 'medium',
  audioMuted: false,
  audioCtx: null,
  activeCamModal: null,
  modalViewMode: 'live', // 'live' | 'event'
  selectedEventIdx: null,

  // ════════════════════════════════════════════════════════════
  //  Bootstrap
  // ════════════════════════════════════════════════════════════
  init() {
    this.initTheme();
    this._initAudio();
    this.bindUIEvents();
    this._fetchInitialAlerts();
    this.connectWS();
    this.startTimestampTicker();
  },

  _initAudio() {
    this.audioMuted = localStorage.getItem('rapidalert_audio_mute') === 'true';
    const btn = document.getElementById('btn-audio-toggle');
    const icon = document.getElementById('audio-toggle-icon');
    if (icon) icon.textContent = this.audioMuted ? '🔇' : '🔊';
    if (btn) {
      btn.title = this.audioMuted ? 'Alert Audio Muted (Click to Unmute)' : 'Alert Audio Active (Click to Mute)';
      btn.addEventListener('click', () => {
        this.audioMuted = !this.audioMuted;
        localStorage.setItem('rapidalert_audio_mute', this.audioMuted ? 'true' : 'false');
        if (icon) icon.textContent = this.audioMuted ? '🔇' : '🔊';
        btn.title = this.audioMuted ? 'Alert Audio Muted (Click to Unmute)' : 'Alert Audio Active (Click to Mute)';
        this._showToast(this.audioMuted ? 'Audio chime muted' : 'Audio chime enabled', 'ok');
        if (!this.audioMuted) {
          this._playAlertChime('medium');
        }
      });
    }
  },

  _playAlertChime(severity = 'high') {
    if (this.audioMuted) return;
    try {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      if (!AudioCtx) return;
      if (!this.audioCtx) this.audioCtx = new AudioCtx();
      if (this.audioCtx.state === 'suspended') {
        this.audioCtx.resume();
      }
      const now = this.audioCtx.currentTime;
      const osc = this.audioCtx.createOscillator();
      const gain = this.audioCtx.createGain();

      const sev = (severity || 'low').toLowerCase();
      if (sev === 'high') {
        osc.type = 'sine';
        osc.frequency.setValueAtTime(880, now);
        osc.frequency.exponentialRampToValueAtTime(587, now + 0.18);
        gain.gain.setValueAtTime(0.18, now);
        gain.gain.exponentialRampToValueAtTime(0.01, now + 0.35);
        osc.connect(gain);
        gain.connect(this.audioCtx.destination);
        osc.start(now);
        osc.stop(now + 0.35);
      } else if (sev === 'medium') {
        osc.type = 'sine';
        osc.frequency.setValueAtTime(587, now);
        osc.frequency.exponentialRampToValueAtTime(784, now + 0.12);
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.exponentialRampToValueAtTime(0.01, now + 0.25);
        osc.connect(gain);
        gain.connect(this.audioCtx.destination);
        osc.start(now);
        osc.stop(now + 0.25);
      }
    } catch (e) {
      console.debug('Web audio chime silenced:', e);
    }
  },

  initTheme() {
    const saved = localStorage.getItem('rapidalert_theme') || localStorage.getItem('theme');
    const isLight = saved === 'light';
    if (isLight) {
      document.documentElement.classList.add('light-mode');
      document.body.classList.add('light-mode');
    } else {
      document.documentElement.classList.remove('light-mode');
      document.body.classList.remove('light-mode');
    }
    const icon = document.getElementById('theme-toggle-icon');
    if (icon) icon.innerText = isLight ? '🌙' : '☀️';
  },

  toggleTheme() {
    const isLight = document.body.classList.toggle('light-mode');
    document.documentElement.classList.toggle('light-mode', isLight);
    const icon = document.getElementById('theme-toggle-icon');
    if (icon) icon.innerText = isLight ? '🌙' : '☀️';
    localStorage.setItem('rapidalert_theme', isLight ? 'light' : 'dark');
    localStorage.setItem('theme', isLight ? 'light' : 'dark');
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
      case 'alert':   return this.onAlert(msg);
      case 'alert_linked': return this.onAlertLinked(msg);
      case 'result_concurrent': return this.onResultConcurrent(msg);
      case 'camera_frame': return this.onCameraFrame(msg);
      case 'scene_shift': return this.onSceneShift(msg);
      case 'sys_metrics': return this.onSysMetrics(msg);
      case 'metrics': return this.onMetrics(msg);
      case 'cameras': return this.onCameras(msg);
      case 'prompts': return this.onPrompts(msg);
      case 'config_updated': return this.onConfigUpdated(msg);
      case 'system_error': return this.onSystemError(msg);
      case 'ping':    break; // keep-alive, no-op
    }
  },

  _sortAlerts() {
    this.alerts.sort((a, b) => (b.ts || 0) - (a.ts || 0));
    this.alertCount = this.alerts.length;
  },

  async _fetchInitialAlerts() {
    try {
      const res = await fetch('/api/alerts?n=60');
      if (res.ok) {
        const alerts = await res.json();
        if (Array.isArray(alerts) && alerts.length > 0) {
          const valid = alerts.filter(a => !a.is_drift && !String(a.observation || '').startsWith('⚡ DINOv2'));
          for (const a of valid) {
            if (!this.alerts.some(existing => existing.id === a.id)) {
              this.alerts.push(a);
            }
          }
          this._sortAlerts();
          this._renderAllAlerts();
        }
      }
    } catch (e) {
      console.warn('Error fetching initial alerts via REST:', e);
    }
  },

  _renderAllAlerts() {
    const feed = document.getElementById('alerts-feed');
    if (!feed) return;

    // Ensure empty placeholder exists at the top
    let empty = document.getElementById('alerts-empty');
    if (!empty) {
      feed.insertAdjacentHTML('afterbegin', `
        <div class="alerts-empty" id="alerts-empty">
          <div class="alerts-empty-icon">🛡️</div>
          <p>System Normal</p>
          <small>Real-time AI incidents and scene shifts will appear here.</small>
        </div>
      `);
      empty = document.getElementById('alerts-empty');
    }

    // Remove existing alert cards
    feed.querySelectorAll('.alert-item').forEach(el => el.remove());

    const af = this.activeAlertFilter || 'medium';
    let visibleCount = 0;
    const frag = document.createDocumentFragment();

    for (const a of this.alerts) {
      const item = this._createAlertElement(a, false);
      const show = this._matchesAlertFilter(a, af);
      item.style.display = show ? '' : 'none';
      if (show) visibleCount++;
      frag.appendChild(item);
    }

    feed.appendChild(frag);
    this._updateEmptyState(visibleCount);
    this._syncAlertCount(visibleCount);
  },

  // ════════════════════════════════════════════════════════════
  //  Message handlers
  // ════════════════════════════════════════════════════════════
  onInit(msg) {
    this.cameras = {};
    for (const cam of (msg.cameras || [])) {
      this.cameras[cam.name] = {
        config: cam,
        results: [],
        lastTs: 0,
        thumbB64: null,
        eventPhotos: [],
        drift: null,
        is_incident: false
      };
    }

    if (msg.thumbnails) {
      for (const [cam, thumbs] of Object.entries(msg.thumbnails)) {
        if (this.cameras[cam] && thumbs && thumbs.length > 0) {
          this.cameras[cam].thumbB64 = thumbs[0];
        }
      }
    }

    if (msg.concurrent_results) {
      for (const [cam, data] of Object.entries(msg.concurrent_results)) {
        if (this.cameras[cam]) {
          if (data.results && data.results.length > 0) {
            this.cameras[cam].results = data.results;
          }
          if (data.thumbnails_b64 && data.thumbnails_b64.length > 0) {
            this.cameras[cam].eventPhotos = data.thumbnails_b64;
          }
        }
      }
    } else if (msg.results) {
      for (const [cam, res] of Object.entries(msg.results)) {
        if (this.cameras[cam]) {
          this.cameras[cam].results = [res];
          this.cameras[cam].lastTs = res.ts || Date.now() / 1000;
        }
      }
    }

    if (msg.drifts) {
      for (const [cam, val] of Object.entries(msg.drifts)) {
        if (this.cameras[cam]) this.cameras[cam].drift = val;
      }
    }

    if (msg.alerts && Array.isArray(msg.alerts)) {
      const valid = msg.alerts.filter(a => !a.is_drift && !String(a.observation || '').startsWith('⚡ DINOv2'));
      for (const a of valid) {
        if (!this.alerts.some(existing => existing.id === a.id)) {
          this.alerts.push(a);
        }
      }
      this._sortAlerts();
      this._renderAllAlerts();
    }

    if (msg.metrics) this._applyMetrics(msg.metrics);
    if (msg.prompts) this._applyPrompts(msg.prompts);
    if (msg.system) {
      this.systemConfig = msg.system;
      const elMajor = document.getElementById('sys-input-dino-major');
      const elMinor = document.getElementById('sys-input-dino-minor');
      const elHb = document.getElementById('sys-input-hb');
      const elCooldown = document.getElementById('sys-input-cooldown');
      const elFollowup = document.getElementById('sys-input-followup-interval');
      const elPersistent = document.getElementById('sys-input-persistent-followup');
      const elClip = document.getElementById('sys-input-clip-enabled');
      const elRolling = document.getElementById('sys-input-clip-rolling');
      const elRetention = document.getElementById('sys-input-clip-retention');

      if (elMajor) elMajor.value = msg.system.dino_major_threshold ?? 0.060;
      if (elMinor) elMinor.value = msg.system.dino_minor_threshold ?? 0.030;
      if (elHb) elHb.value = msg.system.default_heartbeat_sec ?? 35;
      if (elCooldown) elCooldown.value = msg.system.event_cooldown ?? 15;
      if (elFollowup && msg.system.followup_interval_sec != null) elFollowup.value = msg.system.followup_interval_sec;
      if (elPersistent && msg.system.persistent_followup != null) elPersistent.checked = Boolean(msg.system.persistent_followup);
      if (elClip && msg.system.clip_recording_enabled != null) elClip.checked = Boolean(msg.system.clip_recording_enabled);
      if (elRolling && msg.system.clip_rolling_buffer_enabled != null) elRolling.checked = Boolean(msg.system.clip_rolling_buffer_enabled);
      if (elRetention && msg.system.clip_retention_hours != null) elRetention.value = msg.system.clip_retention_hours;
    }

    if (msg.recent_errors && Array.isArray(msg.recent_errors)) {
      this._updateErrorBadge(msg.recent_errors.length);
    }

    // Render fixed camera grid
    this._renderCameraGrid();
  },

  onConfigUpdated(msg) {
    if (msg.system) {
      this.systemConfig = msg.system;
      const elMajor = document.getElementById('sys-input-dino-major');
      const elMinor = document.getElementById('sys-input-dino-minor');
      const elHb = document.getElementById('sys-input-hb');
      const elCooldown = document.getElementById('sys-input-cooldown');
      const elFollowup = document.getElementById('sys-input-followup-interval');
      const elPersistent = document.getElementById('sys-input-persistent-followup');
      const elClip = document.getElementById('sys-input-clip-enabled');
      const elRolling = document.getElementById('sys-input-clip-rolling');
      const elRetention = document.getElementById('sys-input-clip-retention');

      if (elMajor) elMajor.value = msg.system.dino_major_threshold ?? 0.060;
      if (elMinor) elMinor.value = msg.system.dino_minor_threshold ?? 0.030;
      if (elHb) elHb.value = msg.system.default_heartbeat_sec ?? 35;
      if (elCooldown) elCooldown.value = msg.system.event_cooldown ?? 15;
      if (elFollowup && msg.system.followup_interval_sec != null) elFollowup.value = msg.system.followup_interval_sec;
      if (elPersistent && msg.system.persistent_followup != null) elPersistent.checked = Boolean(msg.system.persistent_followup);
      if (elClip && msg.system.clip_recording_enabled != null) elClip.checked = Boolean(msg.system.clip_recording_enabled);
      if (elRolling && msg.system.clip_rolling_buffer_enabled != null) elRolling.checked = Boolean(msg.system.clip_rolling_buffer_enabled);
      if (elRetention && msg.system.clip_retention_hours != null) elRetention.value = msg.system.clip_retention_hours;
    }
    if (msg.cameras) {
      for (const c of msg.cameras) {
        if (this.cameras[c.name]) this.cameras[c.name].config = c;
      }
      this._syncCamTable(msg.cameras);
      this._renderCameraGrid();
    }
    this._showToast('Configuration hot-reloaded!', 'ok');
  },

  onCameraFrame(msg) {
    const { cam, thumbnail_b64 } = msg;
    if (!this.cameras[cam]) return;
    this.cameras[cam].thumbB64 = thumbnail_b64;

    // Ensure live stream is active on camera card
    const cardImg = document.getElementById(`cam-card-img-${this._eid(cam)}`);
    if (cardImg) {
      const streamUrl = `/api/cameras/${encodeURIComponent(cam)}/stream`;
      if (!cardImg.src.includes('/stream')) {
        cardImg.src = streamUrl;
      }
    } else {
      this._renderCamCard(cam);
    }
  },

  onSysMetrics(msg) {
    this._setText('metric-gpu', `${msg.gpu}%`);
    this._setText('metric-cpu', `${msg.cpu}%`);
    this._setText('metric-ram', `${msg.ram}%`);
  },

  onResultConcurrent(msg) {
    const { cam, results, thumbnails_b64, drift, is_incident } = msg;

    if (!this.cameras[cam]) {
      this.cameras[cam] = { config: { name: cam }, results: [], lastTs: 0, thumbB64: null, eventPhotos: [] };
    }

    this.cameras[cam].results = results;
    this.cameras[cam].lastTs = Date.now() / 1000;
    this.cameras[cam].drift = drift;
    this.cameras[cam].is_incident = is_incident;

    // Store event-based photos captured for AI analysis
    if (thumbnails_b64 && thumbnails_b64.length > 0) {
      this.cameras[cam].eventPhotos = thumbnails_b64;
    }

    this._renderCamCard(cam);
  },

  onSceneShift(msg) {
    const { cam, drift } = msg;
    if (this.cameras[cam]) {
      this.cameras[cam].drift = drift;
      this.cameras[cam].is_incident = true;
      if (this.cameras[cam].thumbB64 && (!this.cameras[cam].eventPhotos || this.cameras[cam].eventPhotos.length === 0)) {
        this.cameras[cam].eventPhotos = [this.cameras[cam].thumbB64];
      }
      this._renderCamCard(cam);
    }
  },

  onAlert(msg) {
    const alert = msg.data || msg.alert || msg;
    if (!alert) return;
    // Deduplicate by ID
    if (alert.id && this.alerts.some(a => a.id === alert.id)) return;

    // Link parent if this is a follow-up
    if (alert.parent_id) {
      const parent = this.alerts.find(a => a.id === alert.parent_id);
      if (parent) {
        parent.followup_id = alert.id;
      }
      if (this.activeAlert && this.activeAlert.id === alert.parent_id) {
        this.activeAlert.followup_id = alert.id;
        this._renderPairBanner(this.activeAlert);
      }
    }

    this.alerts.unshift(alert);
    if (this.alerts.length > 200) this.alerts.pop();
    this.alertCount = this.alerts.length;
    this._renderAlert(alert, true);
    this._applyAlertFilter();
    this._syncAlertCount();
    this._playAlertChime(alert.severity || 'high');

    // If alert inspector modal is currently viewing this camera, dynamically refresh related list
    if (this.activeAlert && this.activeAlert.cam === alert.cam) {
      this._renderRelatedAlerts(this.activeAlert);
    }
  },

  onAlertLinked(msg) {
    const parent = this.alerts.find(a => a.id === msg.parent_id);
    if (parent) {
      parent.followup_id = msg.followup_id;
      if (msg.incident_id) parent.incident_id = msg.incident_id;
    }
    if (this.activeAlert && this.activeAlert.id === msg.parent_id) {
      this.activeAlert.followup_id = msg.followup_id;
      this._renderPairBanner(this.activeAlert);
    }
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
    this._syncEmptyState();
    this._syncCamTable(cams);
    this._renderSidebar();
  },

  onPrompts(msg) {
    this._applyPrompts(msg.data || msg);
  },

  // ════════════════════════════════════════════════════════════
  //  Camera Grid Rendering
  // ════════════════════════════════════════════════════════════
  _renderCameraGrid() {
    const grid = document.getElementById('camera-grid');
    const empty = document.getElementById('empty-camera-grid');
    const badge = document.getElementById('cams-active-badge');
    if (!grid) return;

    const camNames = Object.keys(this.cameras);
    let activeCount = 0;
    let visibleCount = 0;

    for (const name of camNames) {
      const cam = this.cameras[name];
      const isEnabled = cam.config?.enabled !== false;
      if (isEnabled) activeCount++;

      this._renderCamCard(name);
      
      const card = document.getElementById(`cam-card-${this._eid(name)}`);
      if (card) {
        const isVisible = this._shouldShowCamCard(name);
        card.style.display = isVisible ? 'flex' : 'none';
        if (isVisible) visibleCount++;
      }
    }

    if (badge) {
      badge.textContent = `${activeCount} / ${camNames.length} Active`;
    }

    if (empty) {
      empty.style.display = visibleCount === 0 ? 'flex' : 'none';
    }
  },

  _shouldShowCamCard(name) {
    const filter = this.activeFilter || 'all';
    if (filter === 'all') return true;
    const cam = this.cameras[name];
    if (!cam) return false;

    const isEnabled = cam.config?.enabled !== false;
    if (filter === 'offline') return !isEnabled;

    const topResult = cam.results?.[0];
    const sev = (topResult?.severity || 'LOW').toUpperCase();
    const saf = (topResult?.safety || 'OK').toUpperCase();

    if (filter === 'incident') {
      return sev === 'HIGH' || saf === 'DANGER' || cam.is_incident;
    }
    if (filter === 'drift') {
      const thresh = cam.config?.threshold ?? (this.systemConfig?.default_threshold || 0.033);
      return (cam.drift !== undefined && cam.drift >= thresh) || cam.is_incident;
    }
    return true;
  },

  _renderCamCard(name) {
    const grid = document.getElementById('camera-grid');
    if (!grid) return;

    const cam = this.cameras[name];
    if (!cam) return;

    let card = document.getElementById(`cam-card-${this._eid(name)}`);
    if (!card) {
      card = document.createElement('div');
      card.className = 'cam-card';
      card.id = `cam-card-${this._eid(name)}`;
      grid.appendChild(card);
    }

    const isEnabled = cam.config?.enabled !== false;
    const topResult = cam.results?.[0];
    const sev = (topResult?.severity || 'LOW').toUpperCase();
    const saf = (topResult?.safety || 'OK').toUpperCase();
    const driftVal = cam.drift !== undefined ? Number(cam.drift).toFixed(4) : '—';
    const thresh = cam.config?.threshold ?? (this.systemConfig?.default_threshold || 0.033);

    // Fault classification & visual glow rings
    card.classList.remove('fault-danger', 'fault-drift', 'fault-offline');
    let statusText = 'LIVE';
    let statusBadgeClass = 'badge-green';

    if (!isEnabled) {
      card.classList.add('fault-offline');
      statusText = 'OFFLINE';
      statusBadgeClass = 'badge-muted';
    } else if (sev === 'HIGH' || saf === 'DANGER') {
      card.classList.add('fault-danger');
      statusText = '🚨 INCIDENT';
      statusBadgeClass = 'badge-red';
    } else if (cam.is_incident || (cam.drift !== undefined && cam.drift >= thresh)) {
      card.classList.add('fault-drift');
      statusText = '⚡ SCENE SHIFT';
      statusBadgeClass = 'badge-cyan';
    } else {
      statusText = '💓 HEALTHY';
      statusBadgeClass = 'badge-green';
    }

    // 1. Continuous Live Real-Time Video Stream (25 FPS MJPEG)
    const liveSrc = `/api/cameras/${encodeURIComponent(name)}/stream`;

    // 2. Dedicated Event-Based Photos Strip (Trigger sequence captured for AI analysis)
    let eventPhotosHtml = '';
    const hasEvents = cam.eventPhotos && cam.eventPhotos.length > 0;
    if (hasEvents) {
      const count = cam.eventPhotos.length;
      const isInc = (sev === 'HIGH' || saf === 'DANGER' || cam.is_incident);
      const isDrift = (cam.drift !== undefined && cam.drift >= thresh);
      const tagLabel = isInc ? '🚨 INCIDENT' : (isDrift ? '⚡ SHIFT' : '📸 CAPTURE');
      const tagCls = isInc ? 'tag-incident' : (isDrift ? 'tag-drift' : 'tag-periodic');

      eventPhotosHtml = `
        <div class="cam-event-strip">
          <div class="cam-event-strip-header">
            <div class="cam-event-title-wrap">
              <span class="cam-event-icon">📸</span>
              <span class="cam-event-title">Event Photos</span>
              <span class="cam-event-count">(${count} captured)</span>
            </div>
            <span class="cam-event-tag ${tagCls}">${tagLabel}</span>
          </div>
          <div class="cam-event-thumbs-grid">
            ${cam.eventPhotos.slice(0, 4).map((b64, idx) => `
              <div class="cam-event-thumb-item" data-cam="${this._esc(name)}" data-idx="${idx}" title="Event Frame #${idx + 1} (t-${count - 1 - idx}) — Click to view in Theater">
                <img src="data:image/jpeg;base64,${b64}" alt="Event Frame ${idx + 1}" />
                <span class="cam-event-thumb-badge">t-${count - 1 - idx}</span>
              </div>
            `).join('')}
          </div>
        </div>
      `;
    } else {
      eventPhotosHtml = `
        <div class="cam-card-no-events">
          <span>📸 Event Photos: No triggers recorded yet</span>
          <span class="cam-monitoring-pill">● Monitoring</span>
        </div>
      `;
    }

    const sevCls = { LOW: 'green', MEDIUM: 'amber', HIGH: 'red' }[sev] || 'muted';
    const safCls = { OK: 'green', WARNING: 'amber', DANGER: 'red' }[saf] || 'muted';
    const obsText = topResult?.observation || 'Awaiting VLM scene understanding analysis…';
    const latencyText = topResult?.latency ? `${Number(topResult.latency).toFixed(2)}s` : '—';
    const e2eText = topResult?.e2e_latency != null ? `${Number(topResult.e2e_latency).toFixed(2)}s` : '--';
    const hasOverride = !!(this.prompts.cameras?.[name]);

    card.innerHTML = `
      <div class="cam-card-header">
        <div class="cam-card-title-wrap">
          <span class="cam-live-dot" style="${!isEnabled ? 'background:var(--text-3); box-shadow:none;' : ''}"></span>
          <span class="cam-card-title">${this._esc(name)}</span>
          ${hasOverride ? `<span title="Custom requirement prompt active" style="color:var(--accent); font-size:0.7rem; font-weight:700; background:var(--accent-glow); padding:1px 5px; border-radius:3px;">✎ CUSTOM</span>` : ''}
        </div>
        <div class="cam-card-header-tags">
          <span class="cam-drift-badge" title="DINOv2 Drift Cosine Metric">⚡ ${driftVal}</span>
          <span class="badge ${statusBadgeClass} badge-sm">${statusText}</span>
        </div>
      </div>

      <div class="cam-card-video" id="cam-video-${this._eid(name)}">
        <img id="cam-card-img-${this._eid(name)}" src="${liveSrc}" class="cam-card-img" alt="${this._esc(name)}" onerror="this.style.opacity='0.4'">
        <div class="cam-live-indicator" id="cam-live-ind-${this._eid(name)}">
          <span class="cam-live-dot ${isEnabled ? 'pulsing' : 'offline'}"></span>
          <span>${isEnabled ? 'LIVE RTSP' : 'OFFLINE'}</span>
        </div>
        <button class="cam-return-live-btn" id="cam-return-live-${this._eid(name)}" title="Return to Real-Time Video Stream">
          ▶ Return to Live
        </button>
      </div>

      ${eventPhotosHtml}

      <div class="cam-card-info">
        <div class="cam-card-badges-row">
          <div style="display: flex; gap: 4px;">
            <span class="badge badge-${safCls} badge-sm">🛡 ${saf}</span>
            <span class="badge badge-${sevCls} badge-sm">⚠ ${sev}</span>
          </div>
          <span style="font-size: 0.68rem; color: var(--text-3); font-family: var(--font-mono);">
            ${topResult?.workers ? `👷 ${this._esc(topResult.workers)}` : ''}
          </span>
        </div>
        <p class="cam-card-obs" title="${this._esc(obsText)}">${this._esc(obsText)}</p>
        <div class="cam-card-footer">
          <span>Infer: ${latencyText}</span>
          <span title="DINOv2 Trigger-to-post latency" style="color:${topResult?.e2e_latency != null ? 'var(--cyan)' : 'var(--text-3)'}; font-weight:600;">
            Trig→Post: ${e2eText}
          </span>
        </div>
      </div>
    `;

    const imgEl = card.querySelector(`#cam-card-img-${this._eid(name)}`);
    const indEl = card.querySelector(`#cam-live-ind-${this._eid(name)}`);
    const returnBtn = card.querySelector(`#cam-return-live-${this._eid(name)}`);

    const resetToLive = () => {
      if (imgEl) {
        imgEl.src = `/api/cameras/${encodeURIComponent(name)}/stream`;
      }
      if (indEl) {
        indEl.innerHTML = `<span class="cam-live-dot ${isEnabled ? 'pulsing' : 'offline'}"></span><span>${isEnabled ? 'LIVE RTSP' : 'OFFLINE'}</span>`;
        indEl.className = 'cam-live-indicator';
      }
      if (returnBtn) {
        returnBtn.style.display = 'none';
      }
      card.querySelectorAll('.cam-event-thumb-item').forEach(t => t.classList.remove('active'));
    };

    if (returnBtn) {
      returnBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        resetToLive();
      });
    }

    const videoEl = card.querySelector('.cam-card-video');
    if (videoEl) {
      videoEl.addEventListener('click', (e) => {
        e.stopPropagation();
        if (returnBtn && returnBtn.style.display === 'inline-flex') {
          resetToLive();
        }
      });
    }

    // In-place event photo inspection: Clicking thumbnail replaces the image right on the card
    card.querySelectorAll('.cam-event-thumb-item').forEach(thumb => {
      thumb.addEventListener('click', (e) => {
        e.stopPropagation();
        const idx = parseInt(thumb.getAttribute('data-idx'), 10);
        if (cam.eventPhotos && cam.eventPhotos[idx] && imgEl) {
          imgEl.src = `data:image/jpeg;base64,${cam.eventPhotos[idx]}`;
          if (indEl) {
            indEl.innerHTML = `📸 EVENT FRAME #${idx + 1} (t-${cam.eventPhotos.length - 1 - idx})`;
            indEl.className = 'cam-live-indicator event-mode';
          }
          if (returnBtn) {
            returnBtn.style.display = 'inline-flex';
          }
          card.querySelectorAll('.cam-event-thumb-item').forEach(t => t.classList.remove('active'));
          thumb.classList.add('active');
        }
      });
    });

    // If modal is open for this camera, refresh its live frame/stats
    if (this.activeCamModal === name) {
      this._updateModalLiveContent(name);
    }
  },

  // Legacy aliases for backward compatibility
  _renderTbRow(name) { this._renderCamCard(name); },
  _renderSidebar() { this._renderCameraGrid(); },

  _applyFilter() {
    this._renderCameraGrid();
  },

  // ════════════════════════════════════════════════════════════
  //  Alerts & Filtering
  // ════════════════════════════════════════════════════════════
  _matchesAlertFilter(alert, filter) {
    if (!alert) return false;

    // Keyword / Fulltext Search Filter
    if (this.alertSearchQuery && this.alertSearchQuery.trim()) {
      const q = this.alertSearchQuery.trim().toLowerCase();
      const match = (
        (alert.observation && alert.observation.toLowerCase().includes(q)) ||
        (alert.activity && alert.activity.toLowerCase().includes(q)) ||
        (alert.machinery && alert.machinery.toLowerCase().includes(q)) ||
        (alert.cam && alert.cam.toLowerCase().includes(q)) ||
        (alert.id && alert.id.toLowerCase().includes(q)) ||
        (alert.incident_id && alert.incident_id.toLowerCase().includes(q)) ||
        (alert.labels && alert.labels.some(l => String(l).toLowerCase().includes(q)))
      );
      if (!match) return false;
    }

    const af = filter || this.activeAlertFilter || 'medium';
    if (af === 'all') return true;

    const sev = String(alert.severity || 'low').trim().toLowerCase();
    const safety = String(alert.safety || '').trim().toLowerCase();
    const isHigh = sev === 'high' || sev === 'extreme' || sev === 'critical' || safety === 'danger';
    const isMedPlus = isHigh || sev === 'medium' || safety === 'warning';

    if (af === 'high') {
      return isHigh;
    }

    if (af === 'medium') {
      return isMedPlus;
    }

    if (af === 'trigger' || af === 'drift') {
      const triggerMode = String(alert.trigger_mode || '').toUpperCase();
      const isTrigger = triggerMode === 'TRIGGER' || Boolean(alert.is_incident);
      const isFollowup = triggerMode === 'FOLLOWUP' || Boolean(alert.is_followup);
      const isDrift = Boolean(alert.is_drift || (alert.drift != null && alert.drift > 0));
      return isTrigger || isFollowup || isDrift;
    }

    return true;
  },

  _createAlertElement(alert, animate = false) {
    const sev = (alert.severity || 'LOW').toLowerCase();
    const safety = (alert.safety || '').toLowerCase();
    const ts  = alert.ts ? new Date(alert.ts * 1000).toLocaleTimeString() : '—';
    const sevCls = { low: 'green', medium: 'amber', high: 'red', extreme: 'red' }[sev] || 'muted';

    const isFollowup = alert.trigger_mode === 'FOLLOWUP' || Boolean(alert.is_followup);
    const isIncident = Boolean(alert.is_incident || alert.trigger_mode === 'TRIGGER') && !isFollowup;
    let triggerTagHtml = '';
    if (alert.trigger_badge) {
      const tagClass = isFollowup ? 'trigger-tag-followup' : (isIncident ? 'trigger-tag-incident' : 'trigger-tag-periodic');
      const tagTitle = isFollowup ? `${alert.delay_sec || 10}s Temporal Follow-Up Outcome Evaluation` : (isIncident ? 'Triggered by DINOv2 Scene Drift Incident' : 'Scheduled Periodic AI Inspection');
      triggerTagHtml = `<span class="alert-trigger-tag ${tagClass}" title="${tagTitle}">${this._esc(alert.trigger_badge)}</span>`;
    } else if (isFollowup) {
      const delay = alert.delay_sec ? `+${Math.round(alert.delay_sec)}s` : '+10s';
      const cycle = alert.cycle && alert.cycle > 1 ? ` #${alert.cycle}` : '';
      triggerTagHtml = `<span class="alert-trigger-tag trigger-tag-followup" title="Temporal Follow-Up Outcome Evaluation">🔄 FOLLOW-UP${cycle} (${delay})</span>`;
    } else if (isIncident) {
      triggerTagHtml = `<span class="alert-trigger-tag trigger-tag-incident" title="Triggered by DINOv2 Scene Drift Incident">⚡ TRIGGER</span>`;
    } else {
      triggerTagHtml = `<span class="alert-trigger-tag trigger-tag-periodic" title="Scheduled Periodic AI Inspection">⏱️ PERIODIC</span>`;
    }

    if (alert.is_fault) {
      triggerTagHtml = `<span class="alert-trigger-tag trigger-tag-offline">FEED FAULT</span>`;
    }

    const eventIdShort = alert.id ? alert.id.replace(/^EVT-/, '') : '';
    const idBadgeHtml = eventIdShort ? `<span class="alert-id-chip" title="${this._esc(alert.id)}">${this._esc(eventIdShort.length > 18 ? '…' + eventIdShort.slice(-14) : eventIdShort)}</span>` : '';

    const item = document.createElement('div');
    item.className = `alert-item sev-${sev || 'low'}${animate ? ' alert-enter' : ''}`;
    item.setAttribute('data-id', alert.id || '');
    item.setAttribute('data-sev', sev);
    item.setAttribute('data-safety', safety);
    item.setAttribute('data-trigger', isFollowup ? 'followup' : (isIncident ? 'trigger' : 'periodic'));
    item.setAttribute('data-is-drift', (alert.is_drift || isIncident || (alert.drift != null && alert.drift > 0)) ? 'true' : 'false');
    item.title = 'Click to inspect this particular alert and incident history';
    item.innerHTML = `
      <div class="alert-header">
        <span class="alert-cam">${this._esc(alert.cam || 'System')}</span>
        ${triggerTagHtml}
        ${idBadgeHtml}
        <span class="badge badge-${sevCls} badge-sm">${sev.toUpperCase()}</span>
        <span class="alert-ts">${ts}</span>
      </div>
      ${alert.thumbnail_b64 ? `
        <div class="alert-thumb-wrap" style="position:relative; margin-bottom:6px;">
          <img class="alert-thumb" src="data:image/jpeg;base64,${alert.thumbnail_b64}" alt="Event photo" style="margin-bottom:0;">
          <span style="position:absolute; bottom:4px; right:4px; background:rgba(0,0,0,0.78); color:#f1f5f9; font-size:0.58rem; font-family:var(--font-mono); font-weight:600; padding:1px 5px; border-radius:3px;">📸 Event Photo</span>
        </div>
      ` : ''}
      <p class="alert-obs">${this._esc(alert.observation || '—')}</p>
    `;

    item.addEventListener('click', () => {
      this.openAlertModal(alert);
    });

    if (animate && (sev === 'high' || sev === 'extreme')) {
      item.classList.add('alert-shake');
      setTimeout(() => item.classList.remove('alert-shake'), 800);
    }

    return item;
  },

  _renderAlert(alert, animate = false) {
    const feed = document.getElementById('alerts-feed');
    if (!feed) return;

    const item = this._createAlertElement(alert, animate);
    const show = this._matchesAlertFilter(alert, this.activeAlertFilter);
    item.style.display = show ? '' : 'none';

    // Insert before the first alert card so empty state stays at the very top if ever needed
    const firstItem = feed.querySelector('.alert-item');
    if (firstItem) {
      feed.insertBefore(item, firstItem);
    } else {
      feed.appendChild(item);
    }

    // Trim feed to 100 alert items maximum without deleting #alerts-empty
    const allItems = feed.querySelectorAll('.alert-item');
    if (allItems.length > 100) {
      for (let i = 100; i < allItems.length; i++) {
        allItems[i].remove();
      }
    }

    this._applyAlertFilter();
  },

  _updateEmptyState(visibleCount) {
    const empty = document.getElementById('alerts-empty');
    if (!empty) return;
    const af = this.activeAlertFilter || 'medium';
    empty.style.display = visibleCount === 0 ? 'flex' : 'none';

    const p = empty.querySelector('p');
    const small = empty.querySelector('small');
    if (p && small) {
      if (this.alerts.length === 0) {
        p.textContent = 'System Normal';
        small.textContent = 'Real-time AI incidents and scene shifts will appear here.';
      } else {
        const labels = {
          all: 'Alerts',
          high: 'High Severity',
          medium: 'Med+ (Medium & High)',
          trigger: 'Trigger / Follow-Up'
        };
        p.textContent = `No ${labels[af] || af} Alerts`;
        small.textContent = `${this.alerts.length} total incident(s) in session. Switch to "All" to view full history.`;
      }
    }
  },

  _applyAlertFilter() {
    const af = this.activeAlertFilter || 'medium';
    const items = document.querySelectorAll('#alerts-feed .alert-item');
    let visibleCount = 0;

    items.forEach(item => {
      const alertId = item.getAttribute('data-id');
      const alert = this.alerts.find(a => a.id === alertId);
      let show = false;
      if (alert) {
        show = this._matchesAlertFilter(alert, af);
      } else {
        const sev = (item.getAttribute('data-sev') || 'low').toLowerCase();
        const safety = (item.getAttribute('data-safety') || '').toLowerCase();
        const trigger = item.getAttribute('data-trigger') || 'periodic';
        const isDrift = item.getAttribute('data-is-drift') === 'true';
        const isHigh = sev === 'high' || sev === 'extreme' || sev === 'critical' || safety === 'danger';
        const isMedPlus = isHigh || sev === 'medium' || safety === 'warning';
        if (af === 'all') show = true;
        else if (af === 'high') show = isHigh;
        else if (af === 'medium') show = isMedPlus;
        else if (af === 'trigger' || af === 'drift') show = (trigger === 'trigger' || trigger === 'followup' || isDrift);
      }

      item.style.display = show ? '' : 'none';
      if (show) visibleCount++;
    });

    this._updateEmptyState(visibleCount);
    this._syncAlertCount(visibleCount);
  },

  _timeAgo(epochTs) {
    if (!epochTs) return '';
    const sec = Math.max(1, Math.floor(Date.now() / 1000 - epochTs));
    if (sec < 60) return `${sec}s ago`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ago`;
    const hrs = Math.floor(min / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
  },

  openAlertModal(alert) {
    if (!alert) return;
    this.activeAlert = alert;

    const sev = (alert.severity || 'LOW').toLowerCase();
    const safety = (alert.safety || '').toLowerCase();
    const ts = alert.ts ? new Date(alert.ts * 1000).toLocaleTimeString() : '—';
    const fullDate = alert.ts ? new Date(alert.ts * 1000).toLocaleDateString() : '';
    const timeAgo = alert.ts ? this._timeAgo(alert.ts) : '';
    const sevCls = { low: 'green', medium: 'amber', high: 'red' }[sev] || 'muted';
    const safeCls = { danger: 'red', warning: 'amber', ok: 'green' }[safety] || 'muted';

    // Header info
    document.getElementById('alert-modal-cam').textContent = alert.cam || 'System Alert';
    document.getElementById('alert-modal-ts').textContent = `${fullDate} ${ts} • ${timeAgo}`;

    // Systematic Event ID Badge in Modal Header
    const eventIdEl = document.getElementById('alert-modal-event-id');
    if (eventIdEl) {
      eventIdEl.textContent = alert.id || 'EVENT';
      eventIdEl.title = `Systematic Event ID: ${alert.id || '—'}`;
    }

    const isFollowup = alert.trigger_mode === 'FOLLOWUP' || alert.is_followup;
    const isIncident = Boolean(alert.is_incident || alert.trigger_mode === 'TRIGGER') && !isFollowup;
    const typeBadge = document.getElementById('alert-modal-badge-type');
    if (typeBadge) {
      if (isFollowup) {
        typeBadge.textContent = '🔄 FOLLOW-UP (+10s)';
        typeBadge.className = 'badge trigger-tag-followup';
        typeBadge.title = '10-Second Post-Incident Temporal Follow-Up';
      } else if (isIncident) {
        typeBadge.textContent = '⚡ TRIGGER';
        typeBadge.className = 'badge badge-amber';
        typeBadge.title = 'Incident triggered by DINOv2 scene drift';
      } else {
        typeBadge.textContent = '⏱️ PERIODIC';
        typeBadge.className = 'badge badge-neutral';
        typeBadge.title = 'Scheduled periodic surveillance check';
      }
    }

    const sevBadge = document.getElementById('alert-modal-badge-sev');
    if (sevBadge) {
      sevBadge.textContent = `SEV: ${sev.toUpperCase()}`;
      sevBadge.className = `badge badge-${sevCls}`;
    }

    // Quick Pair Navigation Banner (Trigger <-> Follow-Up)
    this._renderPairBanner(alert);

    // Observation of THIS particular alert (clean scene analysis, fallback if legacy jargon)
    let obsText = alert.observation || 'No observation recorded for this alert.';
    if (obsText.startsWith('⚡ DINOv2 scene drift')) {
      const camRes = this.cameras[alert.cam]?.results?.[0];
      if (camRes?.observation) {
        obsText = camRes.observation;
      } else {
        obsText = `Scene movement and visual activity detected on ${alert.cam || 'camera feed'}.`;
      }
    }
    document.getElementById('alert-modal-obs').textContent = obsText;

    // Badges of THIS particular alert
    const badges = document.getElementById('alert-modal-badges');
    if (badges) {
      const modeBadge = isFollowup
        ? `<span class="badge trigger-tag-followup">🔄 FOLLOW-UP (+10s)</span>`
        : `<span class="badge badge-${isIncident ? 'amber' : 'neutral'}">${isIncident ? '⚡ TRIGGER' : '⏱️ PERIODIC'}</span>`;
      badges.innerHTML = [
        modeBadge,
        alert.id ? `<span class="badge badge-mono">${this._esc(alert.id)}</span>` : '',
        `<span class="badge badge-${sevCls}">⚠ ${sev.toUpperCase()}</span>`,
        alert.safety ? `<span class="badge badge-${safeCls}">🛡 ${this._esc(alert.safety)}</span>` : '',
        alert.activity && alert.activity !== 'UNKNOWN' && alert.activity !== 'SCENE_SHIFT' ? `<span class="badge badge-muted">⚡ ${this._esc(alert.activity)}</span>` : '',
        alert.workers && alert.workers !== '0' && alert.workers !== '—' ? `<span class="badge badge-neutral">👷 ${this._esc(alert.workers)} workers</span>` : '',
      ].filter(Boolean).join(' ');
    }

    // Metadata of THIS particular alert
    const meta = document.getElementById('alert-modal-meta');
    if (meta) {
      let modeDesc = '⏱️ Scheduled Periodic Inspection';
      if (isFollowup) modeDesc = `🔄 10-Second Post-Incident Follow-Up ${alert.parent_id ? `(Parent: ${alert.parent_id})` : ''}`;
      else if (isIncident) modeDesc = `⚡ DINOv2 Incident Trigger ${alert.drift ? `(Drift: ${Number(alert.drift).toFixed(4)})` : ''}`;

      const rows = [
        ['Event ID', alert.id || '—'],
        ['Incident Group', alert.incident_id || null],
        ['Trigger Mode', modeDesc],
        ['Camera Feed', alert.cam],
        ['Detected At', alert.ts ? new Date(alert.ts * 1000).toLocaleString() : '—'],
        ['Workers Present', alert.workers && alert.workers !== '0' && alert.workers !== '—' ? alert.workers : null],
        ['Machinery', alert.machinery && alert.machinery !== 'None' ? alert.machinery : null],
        ['Cosmos Model', alert.model || 'vrfai/Cosmos-Reason2-8B-NVFP4'],
        ['Evolution', alert.evolution && alert.evolution !== 'None' ? alert.evolution : null],
      ].filter(([, v]) => v != null);
      meta.innerHTML = rows.map(([k, v]) => `<div><strong>${k}:</strong> ${this._esc(String(v))}</div>`).join('');
    }

    // Diagnostic bar for THIS particular alert
    this._setText('alert-stat-drift', alert.drift != null ? Number(alert.drift).toFixed(4) : '—');
    this._setText('alert-stat-latency', alert.latency != null ? `${Number(alert.latency).toFixed(2)}s` : '—');
    this._setText('alert-stat-e2e', alert.e2e_latency != null ? `${Number(alert.e2e_latency).toFixed(2)}s` : '--');
    this._setText('alert-stat-safety', alert.safety || (alert.is_drift ? 'WARNING' : '—'));

    // Primary Image for THIS particular alert (full native resolution)
    const img = document.getElementById('alert-modal-img');
    const noThumb = document.getElementById('alert-modal-no-thumb');
    const indicator = document.getElementById('alert-modal-indicator');
    const frameWrap = document.getElementById('alert-modal-frame-wrap');
    if (frameWrap) {
      frameWrap.classList.remove('is-zoomed');
      const zoomBadge = document.getElementById('alert-modal-zoom-badge');
      if (zoomBadge) zoomBadge.textContent = '🔍 CLICK TO ZOOM (2x)';
    }
    if (indicator) {
      indicator.innerHTML = `📸 CAPTURED AT INCIDENT (${alert.cam || 'Camera'})`;
    }

    const primaryThumb = alert.thumbnail_b64 || (alert.thumbnails_b64 && alert.thumbnails_b64[alert.thumbnails_b64.length - 1]);
    if (primaryThumb) {
      img.src = `data:image/jpeg;base64,${primaryThumb}`;
      img.style.display = 'block';
      if (noThumb) noThumb.style.display = 'none';
    } else {
      img.style.display = 'none';
      if (noThumb) noThumb.style.display = 'flex';
    }

    // Set up click-to-zoom on the alert frame wrap
    if (frameWrap && !frameWrap._zoomWired) {
      frameWrap._zoomWired = true;
      frameWrap.addEventListener('click', (e) => {
        if (e.target.closest('.stream-live-indicator') || e.target.closest('button')) return;
        frameWrap.classList.toggle('is-zoomed');
        const isZoomed = frameWrap.classList.contains('is-zoomed');
        const zoomBadge = document.getElementById('alert-modal-zoom-badge');
        if (zoomBadge) {
          zoomBadge.textContent = isZoomed ? '🔍 2x ZOOM (Click to reset)' : '🔍 CLICK TO ZOOM (2x)';
        }
      });
    }

    // Multi-frame Sequence for THIS particular alert (if available)
    const framesWrap = document.getElementById('alert-modal-frames-wrap');
    const framesStrip = document.getElementById('alert-modal-frames-strip');
    if (framesWrap && framesStrip) {
      if (alert.thumbnails_b64 && alert.thumbnails_b64.length > 1) {
        framesWrap.style.display = 'block';
        framesStrip.innerHTML = '';
        const count = alert.thumbnails_b64.length;
        const defaultLabels = isFollowup
          ? ['t +2.5s', 't +5.0s', 't +7.5s', 't +10.0s (Outcome)']
          : ['t -10.0s', 't -5.0s', 't -2.0s', 't 0.0s (Trigger)'];
        const labels = (alert.labels && alert.labels.length === count) ? alert.labels : defaultLabels;

        alert.thumbnails_b64.forEach((b64, idx) => {
          const isLatest = idx === count - 1;
          const thumbWrap = document.createElement('div');
          thumbWrap.className = `temporal-strip-thumb-wrap ${isLatest ? 'active' : ''}`;
          const labelText = labels[idx] || defaultLabels[idx] || `Frame ${idx + 1}`;
          thumbWrap.title = `${labelText} — Click to inspect`;
          thumbWrap.innerHTML = `
            <img src="data:image/jpeg;base64,${b64}" class="temporal-strip-thumb" alt="${labelText}">
            <span class="temporal-strip-label">${labelText}</span>
          `;
          thumbWrap.addEventListener('click', (e) => {
            e.stopPropagation();
            img.src = `data:image/jpeg;base64,${b64}`;
            framesStrip.querySelectorAll('.temporal-strip-thumb-wrap').forEach(w => w.classList.remove('active'));
            thumbWrap.classList.add('active');
            if (indicator) {
              indicator.innerHTML = `📸 INCIDENT FRAME #${idx + 1} • ${labelText}`;
            }
          });
          framesStrip.appendChild(thumbWrap);
        });
      } else {
        framesWrap.style.display = 'none';
      }
    }

    // Video Clip for THIS alert (if available)
    const videoWrap = document.getElementById('alert-modal-video-wrap');
    const videoEl = document.getElementById('alert-modal-video');
    if (videoWrap && videoEl) {
      if (alert.clip_path) {
        videoWrap.style.display = 'block';
        videoEl.src = alert.clip_path;
        videoEl.load();
        videoEl.play().catch(() => {});
      } else {
        videoWrap.style.display = 'none';
        videoEl.pause();
        videoEl.src = '';
      }
    }

    // Render RELATED ALERTS for this camera
    this._renderRelatedAlerts(alert);

    // Wire up "Switch to Live Feed" button
    const jumpBtn = document.getElementById('btn-alert-jump-live');
    if (jumpBtn) {
      jumpBtn.onclick = () => {
        this.closeAlertModal();
        if (alert.cam) this._openCamModal(alert.cam, null);
      };
    }

    document.getElementById('alert-modal-backdrop').hidden = false;
    document.getElementById('alert-modal').hidden = false;

    // If multi-frame sequence isn't loaded (e.g. from compact WebSocket summary), fetch full detail
    if (alert.id && (!alert.thumbnails_b64 || alert.thumbnails_b64.length <= 1)) {
      fetch(`/api/alerts/${encodeURIComponent(alert.id)}`)
        .then(r => r.ok ? r.json() : null)
        .then(full => {
          if (full && full.thumbnails_b64 && full.thumbnails_b64.length > 0) {
            alert.thumbnails_b64 = full.thumbnails_b64;
            if (full.thumbnail_b64) alert.thumbnail_b64 = full.thumbnail_b64;
            if (this.activeAlert && this.activeAlert.id === alert.id) {
              this.openAlertModal(alert);
            }
          }
        })
        .catch(err => console.warn('Failed to load full alert details:', err));
    }
  },

  _renderPairBanner(alert) {
    const banner = document.getElementById('alert-modal-pair-banner');
    if (!banner) return;

    const isFollowup = alert.trigger_mode === 'FOLLOWUP' || alert.is_followup;
    const isTrigger = alert.trigger_mode === 'TRIGGER' || (alert.is_incident && !isFollowup);

    if (isFollowup && alert.parent_id) {
      const parent = this.alerts.find(a => a.id === alert.parent_id);
      banner.style.display = 'block';
      banner.innerHTML = `
        <div class="alert-pair-nav-inner from-followup">
          <div class="pair-label">
            <span>⚡ Linked Trigger Event:</span>
            <span class="badge badge-mono">${this._esc(alert.parent_id)}</span>
          </div>
          <button type="button" class="btn-pair-jump btn-amber" id="btn-pair-jump-parent">
            ← View Initial Trigger
          </button>
        </div>
      `;
      const btn = document.getElementById('btn-pair-jump-parent');
      if (btn) {
        btn.onclick = () => {
          if (parent) {
            this.openAlertModal(parent);
          } else {
            this.showToast('Initial trigger event not found in current feed memory.', 'info');
          }
        };
      }
    } else if (isTrigger) {
      const followup = alert.followup_id
        ? this.alerts.find(a => a.id === alert.followup_id)
        : this.alerts.find(a => a.parent_id === alert.id || (alert.incident_id && a.incident_id === alert.incident_id && (a.is_followup || a.trigger_mode === 'FOLLOWUP')));

      if (followup) {
        banner.style.display = 'block';
        banner.innerHTML = `
          <div class="alert-pair-nav-inner">
            <div class="pair-label">
              <span>🔄 10s Follow-Up Outcome Available:</span>
              <span class="badge badge-mono">${this._esc(followup.id)}</span>
            </div>
            <button type="button" class="btn-pair-jump btn-cyan" id="btn-pair-jump-followup">
              View 10s Follow-Up →
            </button>
          </div>
        `;
        const btn = document.getElementById('btn-pair-jump-followup');
        if (btn) {
          btn.onclick = () => {
            this.openAlertModal(followup);
          };
        }
      } else {
        const ageSec = alert.ts ? (Date.now() / 1000 - alert.ts) : 999;
        if (ageSec < 25) {
          banner.style.display = 'block';
          banner.innerHTML = `
            <div class="alert-pair-nav-inner pending">
              <div class="pair-label">
                <span class="pulse-dot"></span>
                <span>🔄 10-Second Follow-Up scheduled & capturing temporal outcome...</span>
              </div>
            </div>
          `;
        } else {
          banner.style.display = 'none';
        }
      }
    } else {
      banner.style.display = 'none';
    }
  },

  _renderRelatedAlerts(currentAlert) {
    const listEl = document.getElementById('alert-related-list');
    const countBadge = document.getElementById('alert-related-count');
    if (!listEl) return;

    const related = this.alerts.filter(a =>
      a.cam === currentAlert.cam &&
      !a.is_drift &&
      !String(a.observation || '').startsWith('⚡ DINOv2') &&
      (a.id ? a.id !== currentAlert.id : (a.ts !== currentAlert.ts || a.observation !== currentAlert.observation))
    );

    if (countBadge) {
      countBadge.textContent = `${related.length} Related`;
    }

    if (related.length === 0) {
      listEl.innerHTML = `<div class="alert-related-empty">No other recorded alerts on ${this._esc(currentAlert.cam || 'this camera')}.</div>`;
      return;
    }

    listEl.innerHTML = '';
    for (const r of related) {
      const item = document.createElement('div');
      item.className = 'alert-related-item';
      const sev = (r.severity || 'LOW').toLowerCase();
      const rSevCls = { low: 'green', medium: 'amber', high: 'red' }[sev] || 'muted';
      const rTs = r.ts ? new Date(r.ts * 1000).toLocaleTimeString() : '—';
      const rIsFollowup = r.trigger_mode === 'FOLLOWUP' || r.is_followup;
      const rIsIncident = Boolean(r.is_incident || r.trigger_mode === 'TRIGGER') && !rIsFollowup;
      let tag = '⏱️ PERIODIC';
      let tagCls = 'neutral';
      if (rIsFollowup) {
        tag = '🔄 FOLLOW-UP (+10s)';
        tagCls = 'cyan';
      } else if (rIsIncident) {
        tag = '⚡ TRIGGER';
        tagCls = 'amber';
      }

      item.innerHTML = `
        ${r.thumbnail_b64 ? `<img src="data:image/jpeg;base64,${r.thumbnail_b64}" class="alert-related-thumb" alt="">` : ''}
        <div class="alert-related-info">
          <div class="alert-related-row">
            <span class="badge badge-${tagCls} badge-sm">${tag}</span>
            <span class="badge badge-${rSevCls} badge-sm">${sev.toUpperCase()}</span>
            <span class="alert-related-ts">${rTs}</span>
          </div>
          <span class="alert-related-obs" title="${this._esc(r.observation || '')}">${this._esc(r.observation || '—')}</span>
        </div>
      `;
      item.addEventListener('click', (e) => {
        e.stopPropagation();
        this.openAlertModal(r);
      });
      listEl.appendChild(item);
    }
  },

  closeAlertModal() {
    document.getElementById('alert-modal-backdrop').hidden = true;
    document.getElementById('alert-modal').hidden = true;
    const videoEl = document.getElementById('alert-modal-video');
    if (videoEl) {
      videoEl.pause();
      videoEl.src = '';
    }
    this.activeAlert = null;
  },

  _syncAlertCount(count) {
    const badge = document.getElementById('alert-count');
    if (!badge) return;
    const af = this.activeAlertFilter || 'medium';
    const vis = count !== undefined ? count : (
      this.alerts.filter(a => this._matchesAlertFilter(a, af)).length
    );
    badge.textContent = vis;
    badge.title = `${vis} visible alert(s) under "${af.toUpperCase()}" filter (${this.alerts.length} total in session)`;
    if (vis === 0) {
      badge.style.background = 'var(--bg-card-hover)';
      badge.style.color = 'var(--text-3)';
      badge.style.border = '1px solid var(--border)';
    } else if (af === 'high') {
      badge.style.background = 'var(--red)';
      badge.style.color = '#fff';
      badge.style.border = 'none';
    } else if (af === 'medium') {
      badge.style.background = 'var(--amber)';
      badge.style.color = '#000';
      badge.style.border = 'none';
    } else if (af === 'trigger') {
      badge.style.background = 'var(--cyan)';
      badge.style.color = '#000';
      badge.style.border = 'none';
    } else {
      badge.style.background = 'var(--accent)';
      badge.style.color = '#fff';
      badge.style.border = 'none';
    }
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
    const followupTA = document.getElementById('followup-prompt-ta');
    if (masterTA && masterTA !== document.activeElement) masterTA.value = p.master || '';
    if (followupTA && followupTA !== document.activeElement) followupTA.value = p.followup || '';
  },

  // ════════════════════════════════════════════════════════════
  //  Camera Theater Modal & Prompt Controls
  // ════════════════════════════════════════════════════════════
  _openCamModal(name, selectedEventIdx = null) {
    const cam = this.cameras[name];
    if (!cam) return;

    this.activeCamModal = name;
    this.modalViewMode = (selectedEventIdx !== null) ? 'event' : 'live';
    this.selectedEventIdx = selectedEventIdx;

    const modal = document.getElementById('cam-modal');
    const backdrop = document.getElementById('modal-backdrop');
    if (!modal || !backdrop) return;

    try {
      const nameEl = document.getElementById('modal-cam-name');
      if (nameEl) nameEl.textContent = name;
      this._updateModalFrameView(name);
      this._updateModalLiveContent(name);

      // Populate Normal Context textareas (day / night)
      const camCfgCtx = cam.config || {};
      const ctxDay   = document.getElementById('modal-cam-ctx-day');
      const ctxNight = document.getElementById('modal-cam-ctx-night');
      if (ctxDay)   ctxDay.value   = camCfgCtx.normal_context_day   || '';
      if (ctxNight) ctxNight.value = camCfgCtx.normal_context_night || '';

      // Populate Quick Tune Fields
      const camCfg = cam.config || {};
      const inpMajor = document.getElementById('modal-cam-major-thresh');
      const inpMinor = document.getElementById('modal-cam-minor-thresh');
      const inpHb = document.getElementById('modal-cam-hb');

      const sysMajor = this.systemConfig?.dino_major_threshold || 0.060;
      const sysMinor = this.systemConfig?.dino_minor_threshold || 0.030;

      if (inpMajor) inpMajor.value = camCfg.major_threshold !== undefined ? camCfg.major_threshold : (camCfg.threshold !== undefined ? camCfg.threshold : sysMajor);
      if (inpMinor) inpMinor.value = camCfg.minor_threshold !== undefined ? camCfg.minor_threshold : (camCfg.threshold !== undefined ? Math.max(0.010, camCfg.threshold * 0.5) : sysMinor);
      if (inpHb) inpHb.value = camCfg.heartbeat_sec !== undefined ? camCfg.heartbeat_sec : (this.systemConfig?.default_heartbeat_sec || 35);
    } catch (err) {
      console.error('Error populating cam modal:', err);
    }

    backdrop.removeAttribute('hidden');
    modal.removeAttribute('hidden');
  },

  _updateModalFrameView(name) {
    const cam = this.cameras[name];
    if (!cam) return;

    const img = document.getElementById('modal-frame');
    const loading = document.getElementById('modal-frame-loading');
    const liveIndicator = document.getElementById('modal-live-indicator');
    const btnReturnLive = document.getElementById('btn-modal-return-live');
    const btnLive = document.getElementById('modal-btn-live');
    const btnEvents = document.getElementById('modal-btn-events');

    if (this.modalViewMode === 'event' && cam.eventPhotos && cam.eventPhotos.length > 0) {
      const idx = (this.selectedEventIdx != null && this.selectedEventIdx < cam.eventPhotos.length)
        ? this.selectedEventIdx
        : (cam.eventPhotos.length - 1);
      const b64 = cam.eventPhotos[idx];
      if (img) {
        img.src = `data:image/jpeg;base64,${b64}`;
        img.style.display = 'block';
      }
      if (loading) loading.style.display = 'none';
      if (liveIndicator) {
        liveIndicator.innerHTML = `📸 EVENT FRAME #${idx + 1} (t-${cam.eventPhotos.length - 1 - idx})`;
        liveIndicator.className = 'stream-live-indicator event-mode';
      }
      if (btnReturnLive) btnReturnLive.style.display = 'inline-flex';
      if (btnLive) btnLive.classList.remove('active');
      if (btnEvents) btnEvents.classList.add('active');
    } else {
      this.modalViewMode = 'live';
      this.selectedEventIdx = null;
      if (img) {
        const streamSrc = `/api/cameras/${encodeURIComponent(name)}/stream?width=1280&quality=85`;
        if (img.src !== window.location.origin + streamSrc) {
          img.src = streamSrc;
        }
        img.style.display = 'block';
        if (loading) loading.style.display = 'none';
      }
      if (liveIndicator) {
        liveIndicator.innerHTML = `<span class="live-pulse-dot"></span> LIVE RTSP`;
        liveIndicator.className = 'stream-live-indicator';
      }
      if (btnReturnLive) btnReturnLive.style.display = 'none';
      if (btnLive) btnLive.classList.add('active');
      if (btnEvents) btnEvents.classList.remove('active');
    }
  },

  _updateModalLiveContent(name) {
    const cam = this.cameras[name];
    if (!cam) return;

    const isEnabled = cam.config?.enabled !== false;
    const topResult = cam.results?.[0] || cam.result;
    const sev = (topResult?.severity || 'LOW').toUpperCase();
    const saf = (topResult?.safety || 'OK').toUpperCase();
    const act = (topResult?.activity || 'UNKNOWN').toUpperCase();

    // Badges in title row
    const statusBadge = document.getElementById('modal-cam-status');
    if (statusBadge) {
      if (!isEnabled) {
        statusBadge.textContent = 'OFFLINE';
        statusBadge.className = 'badge badge-muted';
      } else if (cam.is_incident) {
        statusBadge.textContent = '⚡ SCENE TRIGGER';
        statusBadge.className = 'badge badge-red';
      } else {
        statusBadge.textContent = 'LIVE STREAM';
        statusBadge.className = 'badge badge-green';
      }
    }

    const driftBadge = document.getElementById('modal-cam-drift-badge');
    if (driftBadge) {
      driftBadge.textContent = `Drift: ${cam.drift !== undefined ? Number(cam.drift).toFixed(4) : '0.000'}`;
    }

    const ts = cam.lastTs ? new Date(cam.lastTs * 1000).toLocaleTimeString() : '—';
    const tsEl = document.getElementById('modal-cam-ts');
    if (tsEl) tsEl.textContent = `Last analysis: ${ts}`;

    // Event Photos counter
    const countBadge = document.getElementById('modal-event-photos-count');
    if (countBadge) {
      countBadge.textContent = `${cam.eventPhotos?.length || 0}`;
    }

    // Temporal / Event Photos Strip
    const strip = document.getElementById('modal-temporal-strip');
    if (strip) {
      strip.innerHTML = '';
      if (cam.eventPhotos && cam.eventPhotos.length > 0) {
        const count = cam.eventPhotos.length;
        cam.eventPhotos.forEach((b64, idx) => {
          const isSelected = (this.modalViewMode === 'event' && this.selectedEventIdx === idx);
          const thumbWrap = document.createElement('div');
          thumbWrap.className = `temporal-strip-thumb-wrap ${isSelected ? 'active' : ''}`;
          thumbWrap.title = `Event Photo #${idx + 1} (t-${count - 1 - idx}) — Click to inspect`;
          thumbWrap.innerHTML = `
            <img src="data:image/jpeg;base64,${b64}" class="temporal-strip-thumb" alt="Event Frame ${idx + 1}">
            <span class="temporal-strip-label">Frame ${idx + 1} (t-${count - 1 - idx})</span>
          `;
          thumbWrap.addEventListener('click', () => {
            this.modalViewMode = 'event';
            this.selectedEventIdx = idx;
            this._updateModalFrameView(name);
            this._updateModalLiveContent(name);
          });
          strip.appendChild(thumbWrap);
        });
      } else {
        strip.innerHTML = `<div class="temporal-strip-empty">No trigger event photos captured for this feed yet.</div>`;
      }
    }

    // Diagnostic bar
    this._setText('modal-stat-drift', cam.drift !== undefined ? Number(cam.drift).toFixed(4) : '—');
    this._setText('modal-stat-latency', topResult?.latency != null ? `${Number(topResult.latency).toFixed(2)}s` : '—');
    this._setText('modal-stat-e2e', topResult?.e2e_latency != null ? `${Number(topResult.e2e_latency).toFixed(2)}s` : '--');
    this._setText('modal-stat-safety', saf);

    // Scene understanding badges
    const sevBadge = document.getElementById('modal-scene-sev');
    if (sevBadge) {
      const sevCls = { LOW: 'green', MEDIUM: 'amber', HIGH: 'red' }[sev] || 'muted';
      sevBadge.textContent = `⚠ ${sev}`;
      sevBadge.className = `badge badge-${sevCls}`;
    }

    const badgesEl = document.getElementById('modal-badges');
    if (badgesEl) {
      const actCls = { ACTIVE: 'green', IDLE: 'amber', UNKNOWN: 'muted' }[act] || 'muted';
      const safCls = { OK: 'green', WARNING: 'amber', DANGER: 'red' }[saf] || 'muted';
      badgesEl.innerHTML = `
        <span class="badge badge-${actCls}">⚡ ${act}</span>
        <span class="badge badge-neutral">👷 ${this._esc(topResult?.workers || '0')} workers</span>
        <span class="badge badge-${safCls}">🛡 ${saf}</span>
      `;
    }

    // Observation
    const obsEl = document.getElementById('modal-obs');
    if (obsEl) {
      obsEl.textContent = topResult?.observation || 'No VLM analysis received yet.';
    }

    // Meta
    const metaEl = document.getElementById('modal-meta');
    if (metaEl && topResult) {
      metaEl.innerHTML = `
        <span>Model: ${this._esc(topResult.model || 'Cosmos-Nemotron')}</span>
        ${topResult.machinery && topResult.machinery !== 'None' ? `<span>Machinery: ${this._esc(topResult.machinery)}</span>` : ''}
        ${topResult.evolution && topResult.evolution !== 'None' ? `<span>Evolution: ${this._esc(topResult.evolution)}</span>` : ''}
        <span>Timestamp: ${topResult.ts ? new Date(topResult.ts * 1000).toLocaleString() : '—'}</span>
      `;
    }
  },

  _openModal(name) {
    this._openCamModal(name);
  },

  _closeModal() {
    this.activeCamModal = null;
    document.getElementById('modal-backdrop')?.setAttribute('hidden', '');
    document.getElementById('cam-modal')?.setAttribute('hidden', '');
    const img = document.getElementById('modal-frame');
    if (img && img.src.startsWith('blob:')) URL.revokeObjectURL(img.src);
    if (img) img.src = '';
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
    // Sync followup prompt
    const followupTA = document.getElementById('followup-prompt-ta');
    if (followupTA) followupTA.value = this.prompts.followup || '';
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
        <td><input type="number" step="0.005" min="0.005" max="0.5" class="input ctx-thresh" data-cam="${this._esc(cam.name)}"
                   value="${cam.threshold !== undefined ? cam.threshold : (this.systemConfig?.default_threshold || 0.033)}" style="width: 75px; font-family: var(--font-mono);"></td>
        <td><input type="number" step="5" min="5" max="600" class="input ctx-hb" data-cam="${this._esc(cam.name)}"
                   value="${cam.heartbeat_sec !== undefined ? cam.heartbeat_sec : (this.systemConfig?.default_heartbeat_sec || 30)}" style="width: 65px; font-family: var(--font-mono);"></td>
        <td><input type="text" class="input ctx-day" data-cam="${this._esc(cam.name)}"
                   value="${this._esc(cam.normal_context_day || '')}" placeholder="Day context…"></td>
        <td><input type="text" class="input ctx-night" data-cam="${this._esc(cam.name)}"
                   value="${this._esc(cam.normal_context_night || '')}" placeholder="Night context…"></td>
        <td><button class="btn-danger btn-del" data-cam="${this._esc(cam.name)}">🗑</button></td>
      `;
      tbody.appendChild(tr);
    }
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

  async _saveFollowupPrompt() {
    const text = document.getElementById('followup-prompt-ta')?.value;
    if (text == null) return;
    await fetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ followup: text }),
    });
    this._flashSaveFeedback('followup-save-fb', '✓ Saved');
  },

  async _saveModalContext() {
    const name = this.activeCamModal;
    if (!name) return;
    const ctxDay   = document.getElementById('modal-cam-ctx-day')?.value ?? '';
    const ctxNight = document.getElementById('modal-cam-ctx-night')?.value ?? '';
    const fb = document.getElementById('modal-context-fb');
    const cam = this.cameras[name];
    if (!cam) return;
    const cfg = { ...(cam.config || {}), name, normal_context_day: ctxDay, normal_context_night: ctxNight };
    try {
      await this._apiUpsertCamera(cfg);
      if (fb) {
        fb.textContent = '✅ Context Saved!';
        fb.style.color = 'var(--green)';
        setTimeout(() => { if (fb) fb.textContent = ''; }, 3000);
      }
      this._showToast(`Saved scene context for ${name}`, 'ok');
    } catch (err) {
      if (fb) {
        fb.textContent = '❌ Failed to save';
        fb.style.color = 'var(--red)';
      }
    }
  },

  async _addCamera() {
    const name     = document.getElementById('new-cam-name')?.value.trim();
    const url      = document.getElementById('new-cam-url')?.value.trim();
    const ctxDay   = document.getElementById('new-cam-ctx-day')?.value.trim() || '';
    const ctxNight = document.getElementById('new-cam-ctx-night')?.value.trim() || '';
    if (!name || !url) { this._showToast('Enter a name and RTSP URL', 'warn'); return; }
    await this._apiUpsertCamera({ name, url, enabled: true, normal_context_day: ctxDay, normal_context_night: ctxNight });
    document.getElementById('new-cam-name').value = '';
    document.getElementById('new-cam-url').value  = '';
    if (document.getElementById('new-cam-ctx-day')) document.getElementById('new-cam-ctx-day').value = '';
    if (document.getElementById('new-cam-ctx-night')) document.getElementById('new-cam-ctx-night').value = '';
    this._showToast(`Camera "${name}" added`, 'ok');
  },

  async _fetchVLMEndpoints() {
    try {
      const [rHealth, rStats] = await Promise.all([
        fetch('/api/health/vlm').catch(() => null),
        fetch('/api/vlm/stats').catch(() => null),
      ]);
      if (rHealth && rHealth.ok) {
        const data = await rHealth.json();
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
      }
      if (rStats && rStats.ok) {
        const statsData = await rStats.json();
        const shards = statsData.shards || [];
        const g0 = document.getElementById('shard-0-gauge');
        const g1 = document.getElementById('shard-1-gauge');
        if (shards[0] && g0) {
          const s0 = shards[0];
          g0.textContent = `8000: [${s0.in_flight || 0}/${s0.max_concurrent || 4}]`;
          g0.className = s0.healthy ? 'badge badge-green' : 'badge badge-red';
        }
        if (shards[1] && g1) {
          const s1 = shards[1];
          g1.textContent = `8001: [${s1.in_flight || 0}/${s1.max_concurrent || 4}]`;
          g1.className = s1.healthy ? 'badge badge-green' : 'badge badge-red';
        }
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
    const empty = document.getElementById('empty-camera-grid');
    if (!empty) return;
    empty.style.display = Object.keys(this.cameras).length === 0 ? 'flex' : 'none';
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
    const search = document.getElementById('hist-search-input')?.value || '';
    const cam  = document.getElementById('hist-cam-sel')?.value  || '';
    const sev  = document.getElementById('hist-sev-sel')?.value  || '';
    const safe = document.getElementById('hist-safe-sel')?.value || '';
    const offset = this._histPage * this._histLimit;

    const params = new URLSearchParams({ limit: this._histLimit, offset });
    if (search.trim()) params.set('search', search.trim());
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
      tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-3);padding:20px;">No records</td></tr>';
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
        <td style="font-family:var(--font-mono);font-size:0.7rem">${row.latency != null ? Number(row.latency).toFixed(2) + 's' : '—'}</td>
        <td style="font-family:var(--font-mono);font-size:0.7rem;color:${row.e2e_latency != null ? 'var(--cyan)' : 'var(--text-3)'};font-weight:500">${row.e2e_latency != null ? Number(row.e2e_latency).toFixed(2) + 's' : '--'}</td>
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
    // Theme toggle
    document.getElementById('btn-theme-toggle')?.addEventListener('click', () => this.toggleTheme());

    // Settings open/close
    document.getElementById('btn-settings')?.addEventListener('click', () => this._openSettings());
    document.getElementById('btn-close-settings')?.addEventListener('click', () => this._closeSettings());
    document.getElementById('settings-backdrop')?.addEventListener('click', () => this._closeSettings());

    // Diagnostics & Errors open/close
    document.getElementById('btn-errors')?.addEventListener('click', () => this._openErrorsModal());
    document.getElementById('btn-close-errors')?.addEventListener('click', () => this._closeErrorsModal());
    document.getElementById('btn-refresh-errors')?.addEventListener('click', () => this._fetchErrors());
    document.getElementById('btn-clear-errors')?.addEventListener('click', () => this._clearErrors());

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

    // Batch Camera actions in Settings (Enable All / Disable All)
    document.getElementById('btn-enable-all-cams')?.addEventListener('click', async () => {
      const cams = Object.values(this.cameras).map(c => ({
        ...(c.config || { name: c.name }),
        enabled: true
      }));
      try {
        await fetch('/api/cameras/batch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(cams)
        });
        this._showToast('Enabled all cameras!', 'ok');
      } catch (err) {
        console.error('Failed to enable all cameras:', err);
      }
    });

    document.getElementById('btn-disable-all-cams')?.addEventListener('click', async () => {
      const cams = Object.values(this.cameras).map(c => ({
        ...(c.config || { name: c.name }),
        enabled: false
      }));
      try {
        await fetch('/api/cameras/batch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(cams)
        });
        this._showToast('Disabled all cameras!', 'ok');
      } catch (err) {
        console.error('Failed to disable all cameras:', err);
      }
    });

    // Delegated events for Settings Cameras Table
    const camTableBody = document.getElementById('cam-table-body');
    camTableBody?.addEventListener('change', async e => {
      const cam = e.target.dataset.cam;
      if (!cam) return;
      const cfg = { ...(this.cameras[cam]?.config || { name: cam, url: '' }) };
      if (e.target.classList.contains('toggle-enabled')) cfg.enabled = e.target.checked;
      if (e.target.classList.contains('ctx-thresh'))     cfg.threshold = parseFloat(e.target.value);
      if (e.target.classList.contains('ctx-hb'))         cfg.heartbeat_sec = parseFloat(e.target.value);
      if (e.target.classList.contains('ctx-day'))        cfg.normal_context_day = e.target.value;
      if (e.target.classList.contains('ctx-night'))      cfg.normal_context_night = e.target.value;
      await this._apiUpsertCamera(cfg);
      this._showToast(`Updated ${cam}`, 'ok');
    });

    camTableBody?.addEventListener('click', async e => {
      const btn = e.target.closest('.btn-del');
      if (!btn) return;
      const cam = btn.dataset.cam;
      if (cam && confirm(`Remove camera "${cam}"?`)) await this._apiDeleteCamera(cam);
    });

    // Prompt actions
    document.getElementById('btn-save-master')?.addEventListener('click', () => this._saveMasterPrompt());
    document.getElementById('btn-save-followup')?.addEventListener('click', () => this._saveFollowupPrompt());

    // Save System Config
    document.getElementById('btn-save-sys-config')?.addEventListener('click', async () => {
      const dinoMajor = parseFloat(document.getElementById('sys-input-dino-major')?.value);
      const dinoMinor = parseFloat(document.getElementById('sys-input-dino-minor')?.value);
      const hb = parseFloat(document.getElementById('sys-input-hb')?.value);
      const cooldown = parseFloat(document.getElementById('sys-input-cooldown')?.value);
      const followup = parseFloat(document.getElementById('sys-input-followup-interval')?.value);
      const persistent = document.getElementById('sys-input-persistent-followup')?.checked;
      const clipEnabled = document.getElementById('sys-input-clip-enabled')?.checked;
      const clipRolling = document.getElementById('sys-input-clip-rolling')?.checked;
      const clipRetention = parseFloat(document.getElementById('sys-input-clip-retention')?.value);

      try {
        const res = await fetch('/api/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            dino_major_threshold: isNaN(dinoMajor) ? undefined : dinoMajor,
            dino_minor_threshold: isNaN(dinoMinor) ? undefined : dinoMinor,
            default_threshold: isNaN(dinoMajor) ? undefined : dinoMajor,
            default_heartbeat_sec: isNaN(hb) ? undefined : hb,
            event_cooldown: isNaN(cooldown) ? undefined : cooldown,
            followup_interval_sec: isNaN(followup) ? undefined : followup,
            persistent_followup: persistent,
            clip_recording_enabled: clipEnabled,
            clip_rolling_buffer_enabled: clipRolling,
            clip_retention_hours: isNaN(clipRetention) ? undefined : clipRetention,
          })
        });
        if (res.ok) {
          const ind = document.getElementById('sys-config-saved');
          if (ind) {
            ind.style.display = 'inline';
            setTimeout(() => { ind.style.display = 'none'; }, 2500);
          }
          this._showToast('System configuration hot-reloaded!', 'ok');
        }
      } catch (err) {
        console.error('Error saving system config:', err);
      }
    });

    // Theater Modal Save Context (Day/Night)
    document.getElementById('btn-modal-save-context')?.addEventListener('click', async () => {
      const name = this.activeCamModal;
      if (!name) return;
      const ctxDay = document.getElementById('modal-cam-ctx-day')?.value || '';
      const ctxNight = document.getElementById('modal-cam-ctx-night')?.value || '';
      const fb = document.getElementById('modal-context-fb');
      const currentCfg = { ...(this.cameras[name]?.config || { name: name, url: '' }) };
      currentCfg.normal_context_day = ctxDay;
      currentCfg.normal_context_night = ctxNight;
      if (!this.cameras[name]) this.cameras[name] = {};
      this.cameras[name].config = currentCfg;
      await this._apiUpsertCamera(currentCfg);
      if (fb) {
        fb.textContent = '✅ Saved';
        fb.style.color = 'var(--green)';
        setTimeout(() => { fb.textContent = ''; }, 2500);
      }
      this._showToast(`Updated normal context for ${name}`, 'ok');
    });

    // Theater Modal Save Drift/Heartbeat Tune
    document.getElementById('btn-modal-save-tune')?.addEventListener('click', async () => {
      const name = this.activeCamModal;
      if (!name) return;
      const major = parseFloat(document.getElementById('modal-cam-major-thresh')?.value);
      const minor = parseFloat(document.getElementById('modal-cam-minor-thresh')?.value);
      const hb = parseFloat(document.getElementById('modal-cam-hb')?.value);
      const fb = document.getElementById('modal-tune-fb');
      const currentCfg = { ...(this.cameras[name]?.config || { name: name, url: '' }) };
      if (!isNaN(major)) currentCfg.major_threshold = major;
      if (!isNaN(minor)) currentCfg.minor_threshold = minor;
      if (!isNaN(major)) currentCfg.threshold = major;
      if (!isNaN(hb)) currentCfg.heartbeat_sec = hb;
      this.cameras[name].config = currentCfg;
      await this._apiUpsertCamera(currentCfg);
      if (fb) {
        fb.textContent = '✅ Applied';
        fb.style.color = 'var(--green)';
        setTimeout(() => { fb.textContent = ''; }, 2500);
      }
      this._showToast(`Updated tuning for ${name}`, 'ok');
      this._renderCamCard(name);
    });

    // Modal Close handlers & View Switcher handlers
    document.getElementById('btn-close-modal')?.addEventListener('click', () => this._closeModal());
    document.getElementById('modal-backdrop')?.addEventListener('click', () => this._closeModal());

    document.getElementById('modal-btn-live')?.addEventListener('click', () => {
      this.modalViewMode = 'live';
      this.selectedEventIdx = null;
      if (this.activeCamModal) {
        this._updateModalFrameView(this.activeCamModal);
        this._updateModalLiveContent(this.activeCamModal);
      }
    });

    document.getElementById('modal-btn-events')?.addEventListener('click', () => {
      const cam = this.cameras[this.activeCamModal];
      if (cam && cam.eventPhotos && cam.eventPhotos.length > 0) {
        this.modalViewMode = 'event';
        this.selectedEventIdx = 0;
        this._updateModalFrameView(this.activeCamModal);
        this._updateModalLiveContent(this.activeCamModal);
      } else {
        this._showToast('No event photos recorded for this feed yet', 'info');
      }
    });

    document.getElementById('btn-modal-return-live')?.addEventListener('click', () => {
      this.modalViewMode = 'live';
      this.selectedEventIdx = null;
      if (this.activeCamModal) {
        this._updateModalFrameView(this.activeCamModal);
        this._updateModalLiveContent(this.activeCamModal);
      }
    });

    // Scanner actions
    document.getElementById('btn-scan-network')?.addEventListener('click', () => this._runScan());
    document.getElementById('btn-scan-clear')?.addEventListener('click', () => {
      document.getElementById('scan-results-wrap').style.display = 'none';
      document.getElementById('scan-results').innerHTML = '';
      document.getElementById('scan-count').textContent = '0 cameras found';
    });

    // Search inputs
    const alertSearchInput = document.getElementById('alert-search-input');
    if (alertSearchInput) {
      let searchTimeout = null;
      alertSearchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
          this.alertSearchQuery = e.target.value;
          this._renderAllAlerts();
        }, 150);
      });
    }

    const histSearchInput = document.getElementById('hist-search-input');
    if (histSearchInput) {
      histSearchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          this._histPage = 0;
          this._loadHistory();
        }
      });
    }

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
      this.alerts = [];
      this.alertCount = 0;
      this._renderAllAlerts();
      fetch('/api/alerts', { method: 'DELETE' }).catch(() => {});
      this._showToast('Cleared alerts feed', 'ok');
    });

    // Trigger Test Alert
    document.getElementById('btn-test-alert')?.addEventListener('click', async () => {
      try {
        const res = await fetch('/api/alerts/test', { method: 'POST' });
        if (res.ok) {
          const body = await res.json();
          this._showToast('⚡ Triggered test incident alert!', 'ok');
          if (body && body.alert) {
            this.onAlert({ alert: body.alert });
          }
        } else {
          this._showToast('Failed to trigger test alert', 'err');
        }
      } catch (e) {
        this._showToast(`Error: ${e.message}`, 'err');
      }
    });

    // VLM health check button
    document.getElementById('btn-check-vlm')?.addEventListener('click', () => this._fetchVLMEndpoints());

    // Alert modal close
    document.getElementById('btn-close-alert-modal')?.addEventListener('click', () => this.closeAlertModal());
    document.getElementById('alert-modal-backdrop')?.addEventListener('click', () => this.closeAlertModal());

    // Keyboard shortcuts
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') {
        this.closeAlertModal();
        this._closeModal();
        this._closeSettings();
        this._closeErrorsModal();
      }
    });
  },

  // ════════════════════════════════════════════════════════════
  //  System Diagnostics & Error Inspector
  // ════════════════════════════════════════════════════════════
  onSystemError(msg) {
    const err = msg.data;
    this.errorCount = (this.errorCount || 0) + 1;
    this._updateErrorBadge(this.errorCount);
    if (err && (err.severity === 'CRITICAL' || err.severity === 'ERROR')) {
      const target = err.camera ? `[${err.camera}]` : `[${err.component}]`;
      this._showToast(`⚠️ ${target} ${err.effect || err.message}`, 'err');
    }
    const modal = document.getElementById('modal-errors');
    if (modal && modal.style.display !== 'none') {
      this._fetchErrors();
    }
  },

  _updateErrorBadge(count) {
    this.errorCount = count;
    const badge = document.getElementById('error-badge');
    if (badge) {
      if (count > 0) {
        badge.textContent = count > 99 ? '99+' : String(count);
        badge.style.display = 'inline-block';
      } else {
        badge.style.display = 'none';
      }
    }
  },

  _openErrorsModal() {
    const modal = document.getElementById('modal-errors');
    if (!modal) return;
    modal.style.display = 'flex';
    this._fetchErrors();
  },

  _closeErrorsModal() {
    const modal = document.getElementById('modal-errors');
    if (modal) modal.style.display = 'none';
  },

  async _fetchErrors() {
    try {
      const [resList, resSum] = await Promise.all([
        fetch('/api/errors?limit=50'),
        fetch('/api/errors/summary')
      ]);
      if (resList.ok) {
        const data = await resList.json();
        const summary = resSum.ok ? await resSum.json() : null;
        this._renderErrors(data.errors || [], summary);
        this._updateErrorBadge(data.count || 0);
      }
    } catch (e) {
      console.error('Failed to fetch system errors:', e);
    }
  },

  _renderErrors(errors, summary) {
    const container = document.getElementById('errors-list');
    const empty = document.getElementById('empty-errors');
    const elTotal = document.getElementById('err-count-total');
    const elCrit = document.getElementById('err-count-critical');
    const elWarn = document.getElementById('err-count-warnings');

    if (summary) {
      if (elTotal) elTotal.textContent = summary.total_errors || 0;
      if (elCrit) elCrit.textContent = (summary.by_severity?.CRITICAL || 0) + (summary.by_severity?.ERROR || 0);
      if (elWarn) elWarn.textContent = summary.by_severity?.WARNING || 0;
    } else {
      if (elTotal) elTotal.textContent = errors.length;
    }

    if (!container) return;
    container.innerHTML = '';

    if (!errors || errors.length === 0) {
      if (empty) empty.style.display = 'block';
      return;
    }
    if (empty) empty.style.display = 'none';

    for (const err of errors) {
      const sevClass = err.severity === 'CRITICAL' ? 'sev-critical' : (err.severity === 'WARNING' ? 'sev-warning' : '');
      const card = document.createElement('div');
      card.className = `error-card ${sevClass}`;

      const camHtml = err.camera ? `<span class="error-cam-badge">📷 ${this._escapeHtml(err.camera)}</span>` : '';
      const traceId = `trace-${err.id}`;

      card.innerHTML = `
        <div class="error-card-header">
          <div class="error-meta-tags">
            <span class="error-comp-badge">${this._escapeHtml(err.component)}</span>
            ${camHtml}
            <span class="badge ${err.severity === 'WARNING' ? 'badge-warning' : 'badge-danger'} badge-sm">${err.severity}</span>
          </div>
          <span class="error-time">${this._escapeHtml(err.timestamp)}</span>
        </div>
        <div class="error-title">${this._escapeHtml(err.error_type)}: ${this._escapeHtml(err.message)}</div>
        <div class="error-effect-box">
          <span class="error-effect-label">↳ OPERATIONAL IMPACT:</span>
          <span>${this._escapeHtml(err.effect)}</span>
        </div>
        <div class="error-origin">Location: ${this._escapeHtml(err.file)} in ${this._escapeHtml(err.function || 'unknown')}()</div>
        ${err.stack_trace ? `
          <button class="error-trace-toggle" data-target="${traceId}">▶ View Traceback</button>
          <pre class="error-traceback" id="${traceId}">${this._escapeHtml(err.stack_trace)}</pre>
        ` : ''}
      `;

      // Accordion toggle
      const btnTrace = card.querySelector('.error-trace-toggle');
      if (btnTrace) {
        btnTrace.addEventListener('click', (e) => {
          e.stopPropagation();
          const target = document.getElementById(btnTrace.dataset.target);
          if (target) {
            const isHidden = target.style.display === 'none' || !target.style.display;
            target.style.display = isHidden ? 'block' : 'none';
            btnTrace.textContent = isHidden ? '▼ Hide Traceback' : '▶ View Traceback';
          }
        });
      }

      container.appendChild(card);
    }
  },

  async _clearErrors() {
    try {
      const res = await fetch('/api/errors', { method: 'DELETE' });
      if (res.ok) {
        this._updateErrorBadge(0);
        this._showToast('Error log cleared', 'ok');
        this._fetchErrors();
      }
    } catch (e) {
      this._showToast(`Failed to clear errors: ${e.message}`, 'err');
    }
  },
};

document.addEventListener('DOMContentLoaded', () => App.init());
