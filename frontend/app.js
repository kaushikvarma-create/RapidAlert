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
  modalAuditState: 'CLEAN',
  modalViewMode: 'live', // 'live' | 'event'
  selectedEventIdx: null,
  camCurrentPage: 1,
  camsPerPage: parseInt(localStorage.getItem('rapidalert_layout_cams') || '4', 10) === 6 ? 6 : 4,
  activeFollowups: {},
  adminToken: sessionStorage.getItem('rapidalert_admin_token') || null,
  adminUser: sessionStorage.getItem('rapidalert_admin_user') || 'admin',
  isAuthConfigured: true,

  // ════════════════════════════════════════════════════════════
  //  Bootstrap
  // ════════════════════════════════════════════════════════════
  init() {
    this.initTheme();
    this._updateLayoutUi();
    this._initAudio();
    this.bindUIEvents();
    this._bindWakeListeners();
    this._fetchVLMEndpoints();
    this._fetchInitialAlerts();
    this._fetchReportingStats();
    this._fetchReports();
    this._fetchActiveStreams();
    this.connectWS();
    this.startLivenessWatchdog();
    this._startStreamWatchdog();
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
  //  WebSocket & Autonomous Self-Healing
  // ════════════════════════════════════════════════════════════
  connectWS() {
    if (this.ws && (this.ws.readyState === WebSocket.CONNECTING || this.ws.readyState === WebSocket.OPEN)) {
      return;
    }
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    try {
      this.ws = new WebSocket(`${proto}//${location.host}/ws`);
    } catch (e) {
      console.warn('WS construct error:', e);
      return;
    }

    this.ws.onopen = () => {
      this.wsReady = true;
      this._lastWsMsgTime = Date.now();
      this._wsStatus(true);
      this._frameRafPending = {};
    };

    this.ws.onclose = () => {
      this.wsReady = false;
      this._wsStatus(false);
      this._frameRafPending = {};
      if (this._reconnectTimer) clearTimeout(this._reconnectTimer);
      this._reconnectTimer = setTimeout(() => this.connectWS(), 2000);
    };

    this.ws.onerror = () => {
      this.wsReady = false;
      this._wsStatus(false);
    };

    this.ws.onmessage = ({ data }) => {
      this._lastWsMsgTime = Date.now();
      try { this.handleMessage(JSON.parse(data)); }
      catch (e) { console.warn('WS parse error:', e); }
    };
  },

  startLivenessWatchdog() {
    this._lastWsMsgTime = Date.now();
    setInterval(() => {
      const now = Date.now();
      const silenceMs = now - (this._lastWsMsgTime || now);
      // If socket is disconnected, or if no messages arrived for > 6s (e.g. after computer sleep / half-open socket)
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN || silenceMs > 6000) {
        this._wsStatus(false);
        this._frameRafPending = {};
        if (this.ws) {
          try { this.ws.close(); } catch (_) {}
        }
        this.connectWS();
        this._fetchStatusSnapshot();
      }
    }, 3000);
  },

  _bindWakeListeners() {
    // When user unlocks OS / returns to tab after screen sleep
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        this.onTabWake();
      }
    });
    window.addEventListener('focus', () => this.onTabWake());
  },

  onTabWake() {
    this._frameRafPending = {};
    const now = Date.now();
    const silenceMs = now - (this._lastWsMsgTime || 0);
    if (!this.wsReady || silenceMs > 3500) {
      if (this.ws) {
        try { this.ws.close(); } catch (_) {}
      }
      this.connectWS();
    }
    this._fetchStatusSnapshot();
    this._fetchInitialAlerts();
  },

  async _fetchStatusSnapshot() {
    try {
      const res = await fetch('/api/status');
      if (res.ok) {
        const data = await res.json();
        for (const [cam, item] of Object.entries(data)) {
          if (!this.cameras[cam]) {
            this.cameras[cam] = { config: { name: cam }, results: [], lastTs: 0, lastFrameTs: 0, thumbB64: null, eventPhotos: [] };
          }
          if (item && item.ts) {
            this.cameras[cam].lastAnalysisTs = item.ts;
          }
        }
        this._updateAllResults();
      }
    } catch (_) {}
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
      case 'followup_status': return this.onFollowupStatus(msg);
      case 'active_streams_count': return this.onActiveStreamsCount(msg);
      case 'ping':    break; // keep-alive, no-op
    }
  },

  onActiveStreamsCount(msg) {
    const total = msg.active_streams || 0;
    const cams = msg.active_cams || [];
    const badge = document.getElementById('live-streams-pull-badge');
    const textEl = document.getElementById('live-streams-pull-text');
    if (textEl) {
      textEl.textContent = `${total} Stream${total === 1 ? '' : 's'} Pulled`;
    }
    if (badge) {
      badge.classList.toggle('empty', total === 0);
      badge.title = total > 0
        ? `Live Streams Pulled (${total}): ${cams.join(', ')}`
        : 'Zero active video display streams currently pulled.';
    }
  },

  async _fetchActiveStreams() {
    try {
      const res = await fetch('/api/cameras/active-streams');
      if (res.ok) {
        const data = await res.json();
        this.onActiveStreamsCount(data);
      }
    } catch (_) {}
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
    this.cameras[cam].lastFrameTs = Date.now() / 1000;

    const isOnPage = this._isCamOnCurrentPage(cam);
    const isModalOpen = (this.activeCamModal === cam);
    if (!isOnPage && !isModalOpen) {
      return;
    }

    if (!this._frameRafPending) {
      this._frameRafPending = {};
    }
    const lastPending = this._frameRafPending[cam] || 0;
    const now = Date.now();
    if (lastPending && (now - lastPending < 50)) {
      return;
    }
    this._frameRafPending[cam] = now;

    requestAnimationFrame(() => {
      this._frameRafPending[cam] = 0;
      const cardImg = document.getElementById(`cam-card-img-${this._eid(cam)}`);
      if (cardImg && (!cardImg.src || !cardImg.src.includes('/stream'))) {
        cardImg.src = `data:image/jpeg;base64,${thumbnail_b64}`;
        cardImg.style.opacity = '1';
      }
      if (this.activeCamModal === cam && this.modalViewMode === 'live') {
        const modalImg = document.getElementById('modal-frame');
        const loading = document.getElementById('modal-frame-loading');
        if (modalImg && (!modalImg.src || !modalImg.src.includes('/stream'))) {
          modalImg.src = `data:image/jpeg;base64,${thumbnail_b64}`;
          modalImg.style.opacity = '1';
          if (loading) loading.style.display = 'none';
        }
      }
    });
  },

  onSysMetrics(msg) {
    this._setText('metric-gpu', `${msg.gpu}%`);
    this._setText('metric-cpu', `${msg.cpu}%`);
    this._setText('metric-ram', `${msg.ram}%`);
  },

  onFollowupStatus(msg) {
    const { cam, active, cycle, severity, resolved } = msg;
    if (!cam) return;
    if (!this.activeFollowups) this.activeFollowups = {};
    if (active) {
      this.activeFollowups[cam] = {
        active: true,
        cycle: cycle || 1,
        severity: severity || 'HIGH',
        ts: Date.now()
      };
    } else if (resolved || active === false) {
      delete this.activeFollowups[cam];
    }
    this._renderFollowupObservationBanner();
  },

  onResultConcurrent(msg) {
    const { cam, results, thumbnails_b64, drift, is_incident, is_followup } = msg;

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

    // Update active follow-up observation tracker
    if (!this.activeFollowups) this.activeFollowups = {};
    if (is_followup) {
      const topRes = (results && results[0]) || {};
      const sev = (topRes.severity || 'LOW').toUpperCase();
      const saf = (topRes.safety || 'OK').toUpperCase();
      const is_elevated_res = (sev === 'HIGH' || sev === 'EXTREME' || saf === 'DANGER');
      if (!is_elevated_res) {
        delete this.activeFollowups[cam];
      } else {
        this.activeFollowups[cam] = {
          active: true,
          cycle: msg.cycle || 1,
          severity: sev,
          ts: Date.now()
        };
      }
      this._renderFollowupObservationBanner();
    } else if (is_incident) {
      const topRes = (results && results[0]) || {};
      const sev = (topRes.severity || 'LOW').toUpperCase();
      const saf = (topRes.safety || 'OK').toUpperCase();
      const is_elevated_res = (sev === 'HIGH' || sev === 'EXTREME' || saf === 'DANGER');
      if (is_elevated_res) {
        this.activeFollowups[cam] = {
          active: true,
          cycle: 1,
          severity: sev,
          ts: Date.now()
        };
        this._renderFollowupObservationBanner();
      }
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

    // If alert indicates elevated severity or active follow-up, track observation
    const is_elevated = (alert.severity === 'HIGH' || alert.severity === 'EXTREME' || alert.safety === 'DANGER');
    if (alert.cam) {
      if (is_elevated) {
        if (!this.activeFollowups) this.activeFollowups = {};
        this.activeFollowups[alert.cam] = {
          active: true,
          cycle: alert.cycle || 1,
          severity: alert.severity || 'HIGH',
          ts: Date.now()
        };
      } else if (alert.is_followup) {
        if (this.activeFollowups) delete this.activeFollowups[alert.cam];
      }
      this._renderFollowupObservationBanner();
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
        document.getElementById(`cam-card-${this._eid(name)}`)?.remove();
      }
    }

    // Add / update
    for (const cam of cams) {
      if (!this.cameras[cam.name]) {
        this.cameras[cam.name] = { config: cam, result: null, lastTs: 0, thumbB64: null };
        this._renderCamCard(cam.name);
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
  //  Camera Grid Rendering & 4-Stream Pagination
  // ════════════════════════════════════════════════════════════
  _renderCameraGrid(forceRefresh = false) {
    const grid = document.getElementById('camera-grid');
    const empty = document.getElementById('empty-camera-grid');
    const badge = document.getElementById('cams-active-badge');
    if (!grid) return;

    const camNames = Object.keys(this.cameras);
    let activeCount = 0;

    // 1. Gather all cameras matching active filter
    const eligibleCams = [];
    for (const name of camNames) {
      const cam = this.cameras[name];
      const isEnabled = cam.config?.enabled !== false;
      if (isEnabled) activeCount++;

      if (this._shouldShowCamCard(name)) {
        eligibleCams.push(name);
      }
    }

    // 2. Compute pagination bounds
    const totalCams = eligibleCams.length;
    const totalPages = Math.max(1, Math.ceil(totalCams / this.camsPerPage));
    if (this.camCurrentPage > totalPages) this.camCurrentPage = totalPages;
    if (this.camCurrentPage < 1) this.camCurrentPage = 1;

    const startIndex = (this.camCurrentPage - 1) * this.camsPerPage;
    const endIndex = Math.min(startIndex + this.camsPerPage, totalCams);
    const visibleOnPage = new Set(eligibleCams.slice(startIndex, endIndex));

    // 3. Phase 1: Detach streams for cameras NOT on this page or if modal is active
    for (const name of camNames) {
      const isVisible = visibleOnPage.has(name);
      const isEnabled = this.cameras[name]?.config?.enabled !== false;
      const shouldStream = isVisible && isEnabled && !this.activeCamModal;

      const card = document.getElementById(`cam-card-${this._eid(name)}`);
      if (card) {
        card.style.display = isVisible ? 'flex' : 'none';
      }

      if (!shouldStream) {
        this._detachDirectStream(name);
      }
    }

    // 4. Phase 2: Attach hardware-accelerated streams for visible cameras on active page
    for (const name of camNames) {
      this._renderCamCard(name);
      const isVisible = visibleOnPage.has(name);
      const isEnabled = this.cameras[name]?.config?.enabled !== false;
      const shouldStream = isVisible && isEnabled && !this.activeCamModal;

      if (shouldStream) {
        this._attachDirectStream(name, forceRefresh);
      }
    }

    // 5. Update Header Badges & Empty State
    if (badge) {
      badge.textContent = `${activeCount} / ${camNames.length} Active`;
    }

    if (empty) {
      empty.style.display = totalCams === 0 ? 'flex' : 'none';
    }

    // 6. Update Pagination Bar
    this._renderPagination(totalCams, totalPages, startIndex, endIndex);

    // 7. Refresh Active Follow-Up Observation Banner
    this._renderFollowupObservationBanner();
  },

  _getCamSlug(name) {
    return (name || '').trim().toLowerCase().replace(/ /g, '_').replace(/-/g, '_');
  },

  _attachDirectStream(name, forceRefresh = false) {
    const eid = this._eid(name);
    const cardImg = document.getElementById(`cam-card-img-${eid}`);
    if (!cardImg) return;

    const streamUrl = `/api/cameras/${encodeURIComponent(name)}/stream?width=1280&quality=78`;
    const curSrc = cardImg.getAttribute('src') || '';
    if (forceRefresh || !curSrc || !curSrc.includes(`/api/cameras/${encodeURIComponent(name)}/stream`)) {
      cardImg.src = `${streamUrl}&t=${Date.now()}`;
    }
    cardImg.onerror = () => {
      setTimeout(() => {
        if (cardImg.isConnected && this._isCamOnCurrentPage(name) && !this.activeCamModal) {
          cardImg.src = `${streamUrl}&retry=${Date.now()}`;
        }
      }, 1500);
    };
    cardImg.style.opacity = '1';
  },

  _detachDirectStream(name) {
    const eid = this._eid(name);
    const cardImg = document.getElementById(`cam-card-img-${eid}`);
    if (cardImg && (cardImg.src.includes('/stream') || cardImg.src.startsWith('http'))) {
      cardImg.src = '';
      cardImg.removeAttribute('src');
    }
  },

  resyncAllStreams() {
    const btn = document.getElementById('btn-resync-streams');
    if (btn) btn.classList.add('spinning');

    const eligibleCams = Object.keys(this.cameras).filter(n => this._shouldShowCamCard(n));
    const startIndex = (this.camCurrentPage - 1) * this.camsPerPage;
    const endIndex = Math.min(startIndex + this.camsPerPage, eligibleCams.length);
    const visibleOnPage = eligibleCams.slice(startIndex, endIndex);

    for (const name of visibleOnPage) {
      this._attachDirectStream(name, true);
    }

    this._showToast(`🔄 Resynced ${visibleOnPage.length} live camera stream(s)`, 'ok');

    setTimeout(() => {
      if (btn) btn.classList.remove('spinning');
    }, 800);
  },

  _startStreamWatchdog() {
    if (this._streamWatchdogTimer) clearInterval(this._streamWatchdogTimer);

    this._streamWatchdogTimer = setInterval(() => {
      // If modal is active, don't interrupt
      if (this.activeCamModal) return;

      const eligibleCams = Object.keys(this.cameras).filter(n => this._shouldShowCamCard(n));
      const startIndex = (this.camCurrentPage - 1) * this.camsPerPage;
      const endIndex = Math.min(startIndex + this.camsPerPage, eligibleCams.length);
      const visibleOnPage = eligibleCams.slice(startIndex, endIndex);

      for (const name of visibleOnPage) {
        if (this.cameras[name]?.config?.enabled === false) continue;

        const eid = this._eid(name);
        const cardImg = document.getElementById(`cam-card-img-${eid}`);
        if (cardImg) {
          const curSrc = cardImg.getAttribute('src') || '';
          if (!curSrc || !curSrc.includes('/stream')) {
            this._attachDirectStream(name, true);
          }
        }
      }
    }, 4000);
  },

  _detachDirectStream(name) {
    const eid = this._eid(name);
    const videoEl = document.getElementById(`cam-card-video-${eid}`);
    const cardImg = document.getElementById(`cam-card-img-${eid}`);

    if (this._hlsInstances && this._hlsInstances[name]) {
      try { this._hlsInstances[name].destroy(); } catch (e) {}
      delete this._hlsInstances[name];
    }
    if (videoEl) {
      videoEl.removeAttribute('src');
      videoEl.load();
      videoEl.style.display = 'none';
    }
    if (cardImg) {
      cardImg.src = '';
      cardImg.removeAttribute('src');
    }
  },

  _renderPagination(totalCams, totalPages, startIndex, endIndex) {
    const pager = document.getElementById('cam-pagination-bar');
    const textEl = document.getElementById('cam-pagination-text');
    const pillsContainer = document.getElementById('cam-page-pills');
    const btnPrev = document.getElementById('btn-cam-prev-page');
    const btnNext = document.getElementById('btn-cam-next-page');

    if (!pager) return;

    if (totalCams <= this.camsPerPage) {
      pager.style.display = totalCams > 0 ? 'inline-flex' : 'none';
      if (textEl) textEl.textContent = `Page 1 of 1`;
      if (btnPrev) {
        btnPrev.disabled = true;
        btnPrev.style.opacity = '0.5';
      }
      if (btnNext) {
        btnNext.disabled = true;
        btnNext.style.opacity = '0.5';
      }
      if (pillsContainer) {
        pillsContainer.innerHTML = `<button class="cam-page-pill active" data-page="1">1</button>`;
      }
      return;
    }

    pager.style.display = 'inline-flex';
    if (textEl) {
      textEl.textContent = `Page ${this.camCurrentPage} of ${totalPages}`;
      textEl.title = `Showing cameras ${startIndex + 1}–${endIndex} of ${totalCams}`;
    }

    if (btnPrev) {
      btnPrev.disabled = (this.camCurrentPage <= 1);
      btnPrev.style.opacity = (this.camCurrentPage <= 1) ? '0.5' : '1';
    }
    if (btnNext) {
      btnNext.disabled = (this.camCurrentPage >= totalPages);
      btnNext.style.opacity = (this.camCurrentPage >= totalPages) ? '0.5' : '1';
    }

    if (pillsContainer) {
      let pillsHtml = '';
      for (let p = 1; p <= totalPages; p++) {
        pillsHtml += `<button class="cam-page-pill ${p === this.camCurrentPage ? 'active' : ''}" data-page="${p}">${p}</button>`;
      }
      pillsContainer.innerHTML = pillsHtml;
    }
  },

  changePage(delta) {
    const eligibleCams = Object.keys(this.cameras).filter(n => this._shouldShowCamCard(n));
    const totalPages = Math.max(1, Math.ceil(eligibleCams.length / this.camsPerPage));
    let target = this.camCurrentPage + delta;
    if (target < 1) target = 1;
    if (target > totalPages) target = totalPages;
    this.setCamPage(target);
  },

  setLayout(cams) {
    const num = parseInt(cams, 10) === 6 ? 6 : 4;
    this.camsPerPage = num;
    try {
      localStorage.setItem('rapidalert_layout_cams', String(num));
    } catch (e) {}
    this._updateLayoutUi();
    this.camCurrentPage = 1;
    this._renderCameraGrid();
  },

  _updateLayoutUi() {
    const grid = document.getElementById('camera-grid');
    if (grid) {
      grid.classList.remove('layout-4', 'layout-6');
      grid.classList.add(`layout-${this.camsPerPage}`);
    }
    const btn4 = document.getElementById('btn-layout-4');
    const btn6 = document.getElementById('btn-layout-6');
    if (btn4 && btn6) {
      btn4.classList.toggle('active', this.camsPerPage === 4);
      btn6.classList.toggle('active', this.camsPerPage === 6);
    }
  },

  setCamPage(page) {
    this.camCurrentPage = page;
    this._updateLayoutUi();
    this._renderCameraGrid(true);
  },

  _isCamOnCurrentPage(name) {
    if (!this._shouldShowCamCard(name)) return false;
    const eligibleCams = Object.keys(this.cameras).filter(n => this._shouldShowCamCard(n));
    const startIndex = (this.camCurrentPage - 1) * this.camsPerPage;
    const endIndex = startIndex + this.camsPerPage;
    const pageSlice = eligibleCams.slice(startIndex, endIndex);
    return pageSlice.includes(name);
  },

  _getCamPage(name) {
    const eligibleCams = Object.keys(this.cameras).filter(n => this._shouldShowCamCard(n));
    const idx = eligibleCams.indexOf(name);
    if (idx === -1) return 1;
    return Math.floor(idx / this.camsPerPage) + 1;
  },

  jumpToCamera(cam) {
    if (!cam) return;
    const targetPage = this._getCamPage(cam);
    if (this.camCurrentPage !== targetPage) {
      this.setCamPage(targetPage);
    }
    setTimeout(() => {
      const eid = this._eid(cam);
      const card = document.getElementById(`cam-card-${eid}`);
      if (card) {
        card.scrollIntoView({ behavior: 'smooth', block: 'center' });
        card.classList.remove('cam-card-targeted');
        void card.offsetWidth; // trigger reflow
        card.classList.add('cam-card-targeted');
        setTimeout(() => card.classList.remove('cam-card-targeted'), 4500);
      }
    }, 120);
  },

  _renderFollowupObservationBanner() {
    const banner = document.getElementById('followup-observation-banner');
    const container = document.getElementById('followup-chips-list');
    if (!banner || !container) return;

    if (!this.activeFollowups) this.activeFollowups = {};
    const now = Date.now();
    const activeCams = Object.entries(this.activeFollowups)
      .filter(([cam, fu]) => fu && fu.active && (now - fu.ts < 180000))
      .map(([cam, fu]) => ({ cam, ...fu }));

    if (activeCams.length === 0) {
      banner.style.display = 'none';
      container.innerHTML = '';
      return;
    }

    banner.style.display = 'block';
    container.innerHTML = activeCams.map(item => {
      const page = this._getCamPage(item.cam);
      const cycleText = item.cycle ? `Cycle #${item.cycle} Observation` : 'Follow-up Active';
      return `
        <div class="followup-flashing-chip" data-cam="${this._esc(item.cam)}" title="Click to navigate directly to ${this._esc(item.cam)} on Page ${page}">
          <span class="followup-chip-cam">🚨 ${this._esc(item.cam)}</span>
          <span class="followup-chip-page">PAGE ${page}</span>
          <span class="followup-chip-cycle">${cycleText}</span>
          <span class="followup-chip-jump">⚡ Focus Stream</span>
        </div>
      `;
    }).join('');
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

    const eid = this._eid(name);
    let card = document.getElementById(`cam-card-${eid}`);
    const isEnabled = cam.config?.enabled !== false;
    const topResult = cam.results?.[0];
    const sev = (topResult?.severity || 'LOW').toUpperCase();
    const saf = (topResult?.safety || 'OK').toUpperCase();
    const driftVal = cam.drift !== undefined ? Number(cam.drift).toFixed(4) : '—';
    const thresh = cam.config?.threshold ?? (this.systemConfig?.default_threshold || 0.033);
    const hasOverride = !!(this.prompts.cameras?.[name]);

    const isVisible = this._isCamOnCurrentPage(name);

    let statusText = 'LIVE';
    let statusBadgeClass = 'badge-green';

    if (!isEnabled) {
      statusText = 'OFFLINE';
      statusBadgeClass = 'badge-muted';
    } else if (sev === 'HIGH' || saf === 'DANGER') {
      statusText = '🚨 INCIDENT';
      statusBadgeClass = 'badge-red';
    } else if (cam.is_incident || (cam.drift !== undefined && cam.drift >= thresh)) {
      statusText = '⚡ SCENE SHIFT';
      statusBadgeClass = 'badge-cyan';
    } else {
      statusText = '💓 HEALTHY';
      statusBadgeClass = 'badge-green';
    }

    const sevCls = { LOW: 'green', MEDIUM: 'amber', HIGH: 'red' }[sev] || 'muted';
    const safCls = { OK: 'green', WARNING: 'amber', DANGER: 'red' }[saf] || 'muted';
    const isWarmup = topResult?.verdict === 'WARMUP' || (topResult?.observation && topResult.observation.includes('Initializing'));
    const obsText = isWarmup ? '⏳ VLM Model Initializing (Loading weights into GPU memory)...' : (topResult?.observation || 'Awaiting VLM scene understanding analysis…');
    const latencyText = topResult?.latency ? `${Number(topResult.latency).toFixed(2)}s` : '—';
    const e2eText = topResult?.e2e_latency != null ? `${Number(topResult.e2e_latency).toFixed(2)}s` : '--';

    // ── Build Initial Card DOM Structure Once ─────────────────────
    if (!card) {
      card = document.createElement('div');
      card.className = 'cam-card';
      card.id = `cam-card-${eid}`;
      card.setAttribute('data-cam', name);
      card.style.cursor = 'pointer';

      card.innerHTML = `
        <div class="cam-card-header">
          <div class="cam-card-title-wrap">
            <span class="cam-live-dot" id="cam-dot-${eid}"></span>
            <span class="cam-card-title">${this._esc(name)}</span>
            <span id="cam-custom-badge-${eid}" style="display:none; color:var(--accent); font-size:0.7rem; font-weight:700; background:var(--accent-glow); padding:1px 5px; border-radius:3px;">✎ CUSTOM</span>
          </div>
          <div class="cam-card-header-tags">
            <span class="cam-drift-badge" id="cam-drift-${eid}" title="DINOv2 Drift Cosine Metric">⚡ —</span>
            <span class="badge badge-sm" id="cam-status-${eid}">LIVE</span>
          </div>
        </div>

        <div class="cam-card-video" id="cam-video-${eid}">
          <img id="cam-card-img-${eid}" class="cam-card-img" alt="${this._esc(name)}" onerror="this.style.opacity='0.4'">
          <div class="cam-live-indicator">
            <span class="cam-live-dot" id="cam-live-indicator-dot-${eid}"></span>
            <span id="cam-live-text-${eid}">LIVE RTSP</span>
          </div>
          <div class="cam-hover-overlay">
            <span>🔍 Inspect Live Feed &amp; Set Prompts</span>
          </div>
        </div>

        <div id="cam-event-strip-container-${eid}"></div>

        <div class="cam-card-info">
          <div class="cam-card-badges-row">
            <div style="display: flex; gap: 4px;">
              <span class="badge badge-sm" id="cam-badge-saf-${eid}">🛡 OK</span>
              <span class="badge badge-sm" id="cam-badge-sev-${eid}">⚠ LOW</span>
            </div>
            <span style="font-size: 0.68rem; color: var(--text-3); font-family: var(--font-mono);" id="cam-workers-${eid}"></span>
          </div>
          <p class="cam-card-obs" id="cam-obs-${eid}">Awaiting VLM scene understanding analysis…</p>
          <div class="cam-card-footer">
            <span id="cam-infer-${eid}">Infer: —</span>
            <span id="cam-e2e-${eid}" title="DINOv2 Trigger-to-post latency" style="color:var(--text-3); font-weight:600;">Trig→Post: --</span>
          </div>
        </div>
      `;

      grid.appendChild(card);
    }

    // ── Targeted DOM Updates (Never destroy img elements or streaming sockets) ──
    card.classList.remove('fault-danger', 'fault-drift', 'fault-offline');
    if (!isEnabled) {
      card.classList.add('fault-offline');
    } else if (sev === 'HIGH' || saf === 'DANGER') {
      card.classList.add('fault-danger');
    } else if (cam.is_incident || (cam.drift !== undefined && cam.drift >= thresh)) {
      card.classList.add('fault-drift');
    }

    card.style.display = isVisible ? 'flex' : 'none';

    const dotEl = document.getElementById(`cam-dot-${eid}`);
    if (dotEl) dotEl.style.background = !isEnabled ? 'var(--text-3)' : '';

    const customEl = document.getElementById(`cam-custom-badge-${eid}`);
    if (customEl) customEl.style.display = hasOverride ? 'inline-block' : 'none';

    const driftEl = document.getElementById(`cam-drift-${eid}`);
    if (driftEl) driftEl.textContent = `⚡ ${driftVal}`;

    const statusEl = document.getElementById(`cam-status-${eid}`);
    if (statusEl) {
      statusEl.className = `badge ${statusBadgeClass} badge-sm`;
      statusEl.textContent = statusText;
    }

    const liveDot = document.getElementById(`cam-live-indicator-dot-${eid}`);
    if (liveDot) liveDot.className = `cam-live-dot ${isEnabled ? 'pulsing' : 'offline'}`;

    const liveText = document.getElementById(`cam-live-text-${eid}`);
    if (liveText) liveText.textContent = isEnabled ? 'LIVE RTSP' : 'OFFLINE';

    // Event Photos Strip
    const eventContainer = document.getElementById(`cam-event-strip-container-${eid}`);
    if (eventContainer) {
      const hasEvents = cam.eventPhotos && cam.eventPhotos.length > 0;
      if (hasEvents) {
        const count = cam.eventPhotos.length;
        const isInc = (sev === 'HIGH' || saf === 'DANGER' || cam.is_incident);
        const isDrift = (cam.drift !== undefined && cam.drift >= thresh);
        const tagLabel = isInc ? '🚨 INCIDENT' : (isDrift ? '⚡ SHIFT' : '📸 CAPTURE');
        const tagCls = isInc ? 'tag-incident' : (isDrift ? 'tag-drift' : 'tag-periodic');

        eventContainer.innerHTML = `
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
        eventContainer.innerHTML = `
          <div class="cam-card-no-events">
            <span>📸 Event Photos: No triggers recorded yet</span>
            <span class="cam-monitoring-pill">● Monitoring</span>
          </div>
        `;
      }
    }

    const safBadge = document.getElementById(`cam-badge-saf-${eid}`);
    if (safBadge) {
      safBadge.className = `badge badge-${safCls} badge-sm`;
      safBadge.textContent = `🛡 ${saf}`;
    }

    const sevBadge = document.getElementById(`cam-badge-sev-${eid}`);
    if (sevBadge) {
      sevBadge.className = `badge badge-${sevCls} badge-sm`;
      sevBadge.textContent = `⚠ ${sev}`;
    }

    const workersEl = document.getElementById(`cam-workers-${eid}`);
    if (workersEl) workersEl.textContent = topResult?.workers ? `👷 ${topResult.workers}` : '';

    const obsEl = document.getElementById(`cam-obs-${eid}`);
    if (obsEl) {
      obsEl.textContent = obsText;
      obsEl.title = obsText;
    }

    const inferEl = document.getElementById(`cam-infer-${eid}`);
    if (inferEl) inferEl.textContent = `Infer: ${latencyText}`;

    const e2eEl = document.getElementById(`cam-e2e-${eid}`);
    if (e2eEl) {
      e2eEl.textContent = `Trig→Post: ${e2eText}`;
      e2eEl.style.color = topResult?.e2e_latency != null ? 'var(--cyan)' : 'var(--text-3)';
    }

    // Ensure direct hardware live stream is active for visible cameras
    if (isVisible && isEnabled && !this.activeCamModal) {
      this._attachDirectStream(name);
    }

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
        ['Classification Reasoning', alert.reasoning && alert.reasoning !== 'None' ? alert.reasoning : null],
        ['🚨 Priority Flags (Checklist A)', alert.priority_flags && alert.priority_flags !== '[]' && alert.priority_flags !== '' ? alert.priority_flags : null],
        ['✅ Routine Flags (Checklist B)',  alert.routine_flags  && alert.routine_flags  !== '[]' && alert.routine_flags  !== '' ? alert.routine_flags  : null],
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

    if (m.vlm_shards) {
      this._updateVlmShardsUI({ shards: m.vlm_shards, is_mig: m.is_mig, mode: m.vlm_mode });
    }
  },

  _updateVlmShardsUI(data) {
    if (!data) return;
    const shards = Array.isArray(data) ? data : (data.shards || []);
    if (!shards.length) return;

    let isMig = false;
    if (typeof data.is_mig === 'boolean') {
      isMig = data.is_mig;
    } else if (typeof data.mode === 'string') {
      isMig = data.mode.toLowerCase() === 'mig';
    } else {
      isMig = shards.some(s => s.is_mig || (s.mig_uuid && s.mig_uuid.length > 0));
    }

    const modeLabel = document.getElementById('vlm-mode-label');
    if (modeLabel) {
      modeLabel.textContent = isMig ? 'MIG' : 'SHARED';
      modeLabel.title = isMig ? 'Multi-Instance GPU Partitioning Active' : 'Shared GPU Shards (Non-MIG)';
    }

    const pillDot = document.getElementById('vlm-pill-dot');
    if (pillDot) {
      const allHealthy = shards.every(s => s.healthy);
      const anyHealthy = shards.some(s => s.healthy);
      pillDot.className = 'vlm-pill-dot ' + (allHealthy ? '' : (anyHealthy ? 'dot-warming' : 'dot-offline'));
    }

    const container = document.getElementById('vlm-shards-list');
    if (container) {
      container.innerHTML = shards.map((s, idx) => {
        const inflight = (s.inflight !== undefined) ? s.inflight : (s.in_flight !== undefined ? s.in_flight : 0);
        const maxC = s.max_concurrent || 4;
        const port = s.port || (s.url ? s.url.split(':').pop() : idx);
        const queued = s.queued || 0;
        const isHealthy = Boolean(s.healthy);

        let badgeClass = 'badge-mono';
        let labelContent = '';
        if (!isHealthy) {
          badgeClass = 'badge-amber';
          labelContent = `${port}: ⏳ Warming`;
        } else if (inflight > 0) {
          badgeClass = 'badge-amber';
          const qText = queued > 0 ? ` +${queued}q` : '';
          labelContent = `⚡ ${port}: [${inflight}/${maxC}${qText}]`;
        } else {
          badgeClass = 'badge-green';
          const qText = queued > 0 ? ` +${queued}q` : '';
          labelContent = `✓ ${port}: [${inflight}/${maxC}${qText}]`;
        }

        const qText = queued > 0 ? ` +${queued}q` : '';
        const latText = s.avg_latency_ms ? ` (${s.avg_latency_ms}ms)` : '';
        const modelText = s.model ? ` | Model: ${s.model}` : '';
        const statusTitle = isHealthy ? `Online | In-flight: ${inflight}/${maxC}${qText}${latText}${modelText}` : 'Warming Up / Loading Weights into VRAM';
        return `<span id="shard-${idx}-gauge" class="badge ${badgeClass}" style="padding: 1px 6px; font-size: 0.68rem; transition: background 0.2s ease;" title="${this._esc(s.url)} | ${statusTitle} | Weight: ${s.weight ?? 1}">${labelContent}</span>`;
      }).join('');
    }

    // Also live-refresh inspector if currently open
    const modal = document.getElementById('modal-vlm-inspector');
    if (modal && modal.style.display !== 'none' && data.shards) {
      this._renderVlmInspector(data);
    }
  },

  _openVlmInspector() {
    const modal = document.getElementById('modal-vlm-inspector');
    if (!modal) return;
    modal.style.display = 'flex';
    this._fetchVlmContainers();
    if (this._vlmPollTimer) clearInterval(this._vlmPollTimer);
    this._vlmPollTimer = setInterval(() => {
      if (modal.style.display !== 'none') {
        this._fetchVlmContainers();
      } else {
        clearInterval(this._vlmPollTimer);
        this._vlmPollTimer = null;
      }
    }, 2000);
  },

  _closeVlmInspector() {
    const modal = document.getElementById('modal-vlm-inspector');
    if (modal) modal.style.display = 'none';
    if (this._vlmPollTimer) {
      clearInterval(this._vlmPollTimer);
      this._vlmPollTimer = null;
    }
  },

  async _fetchVlmContainers() {
    try {
      const res = await fetch('/api/vlm/containers');
      if (res.ok) {
        const data = await res.json();
        this._renderVlmInspector(data);
      }
    } catch (e) {
      console.warn('Error fetching VLM container telemetry:', e);
    }
  },

  async _probeVlmShards() {
    const btn = document.getElementById('btn-vlm-probe-now');
    if (btn) {
      btn.disabled = true;
      btn.textContent = '⏳ Probing...';
    }
    try {
      const res = await fetch('/api/vlm/probe', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        this._showToast('⚡ Live vLLM shard latency probe complete', 'ok');
        this._fetchVlmContainers();
      } else {
        this._showToast('Failed to probe vLLM shards', 'err');
      }
    } catch (e) {
      this._showToast(`Probe error: ${e.message}`, 'err');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = '🔄 Live Probe';
      }
    }
  },

  _renderVlmInspector(data) {
    if (!data) return;
    const shards = data.shards || [];
    const isMig = Boolean(data.is_mig);

    // Summary banner
    const elMode = document.getElementById('vlm-sum-mode');
    const elCompleted = document.getElementById('vlm-sum-completed');
    const elErrors = document.getElementById('vlm-sum-errors');
    const elModel = document.getElementById('vlm-sum-model');

    if (elMode) elMode.textContent = isMig ? 'MIG Partitioned (A100)' : 'Shared GPU Shards';
    if (elCompleted) elCompleted.textContent = (data.total_inferences ?? shards.reduce((acc, s) => acc + (s.completed || 0), 0)).toLocaleString();
    if (elErrors) elErrors.textContent = (data.total_errors ?? shards.reduce((acc, s) => acc + (s.errors || 0), 0)).toLocaleString();
    if (elModel && shards.length > 0) elModel.textContent = shards[0].model || 'vrfai/Cosmos-Reason2-8B-NVFP4';

    // Render shard cards
    const cardsWrap = document.getElementById('vlm-shards-cards');
    if (cardsWrap) {
      cardsWrap.innerHTML = shards.map((s, idx) => {
        const isHealthy = Boolean(s.healthy);
        const inflight = s.in_flight ?? s.inflight ?? 0;
        const maxC = s.max_concurrent || 4;
        const queued = s.queued || 0;
        const port = s.port || (s.url ? s.url.split(':').pop() : idx);
        const cName = s.container_name || `rapidalert_vllm_${port === '8000' ? 0 : 1}`;
        const dInfo = s.container || {};
        const dStatus = dInfo.status || (isHealthy ? 'Up (Running)' : 'Warming Up / Offline');
        const smCount = s.sm_count ? `${s.sm_count} SMs` : (s.mig_profile ? `${s.mig_profile}` : 'Shared');
        const gpuMem = s.gpu_utilization ? `${Math.round(s.gpu_utilization * 100)}% VRAM` : '—';
        const loadScore = s.load_score !== undefined ? s.load_score.toFixed(2) : ((inflight * 2) + queued).toFixed(2);
        const loadPercent = Math.min(100, Math.max(8, Math.round(((inflight + (queued * 0.5)) / maxC) * 100)));

        const activeJobs = s.active_jobs || [];

        // Concurrency slots HTML
        const slotsHtml = Array.from({ length: maxC }).map((_, slotIdx) => {
          const job = activeJobs[slotIdx];
          if (job) {
            return `
              <div class="vlm-slot-box slot-busy" title="Active inference for ${this._esc(job.cam)} (${job.elapsed_s}s elapsed)">
                <span class="vlm-slot-dot"></span>
                <span>⚡ ${this._esc(job.cam)}</span>
              </div>
            `;
          } else if (slotIdx < inflight) {
            return `
              <div class="vlm-slot-box slot-busy" title="Inference active in slot #${slotIdx + 1}">
                <span class="vlm-slot-dot"></span>
                <span>⚡ Slot #${slotIdx + 1}</span>
              </div>
            `;
          } else {
            return `
              <div class="vlm-slot-box slot-idle" title="Slot #${slotIdx + 1} is idle and ready for requests">
                <span class="vlm-slot-dot"></span>
                <span>⚪ Idle</span>
              </div>
            `;
          }
        }).join('');

        const cardCls = isHealthy ? 'shard-healthy' : 'shard-warning';
        const statusBadge = isHealthy
          ? `<span class="badge badge-green">🟢 Online</span>`
          : `<span class="badge badge-amber">⏳ Warming Up</span>`;

        return `
          <div class="vlm-shard-card ${cardCls}">
            <div class="vlm-shard-header">
              <div class="vlm-shard-title-wrap">
                <span class="vlm-shard-title">Shard ${idx}: Port ${port}</span>
                ${statusBadge}
              </div>
              <span class="vlm-shard-url">${this._esc(s.url)}</span>
            </div>

            <!-- Concurrency Slots -->
            <div class="vlm-slots-section">
              <div class="vlm-slots-header">
                <span>Concurrency Execution Slots (${inflight}/${maxC} Active)</span>
                <span>${queued > 0 ? `+${queued} Queued` : 'Queue Empty'}</span>
              </div>
              <div class="vlm-slots-list">
                ${slotsHtml}
              </div>
            </div>

            <!-- Dynamic Load Meter -->
            <div class="vlm-meter-wrap">
              <div class="vlm-meter-header">
                <span>Weighted Load Score: <strong>${loadScore}</strong></span>
                <span>Weight: ${s.weight ?? 1}x</span>
              </div>
              <div class="vlm-meter-bar">
                <div class="vlm-meter-fill ${loadPercent > 80 ? 'meter-warn' : ''}" style="width: ${loadPercent}%;"></div>
              </div>
            </div>

            <!-- Telemetry Metrics Grid -->
            <div class="vlm-metrics-mini">
              <div class="vlm-m-item">
                <span class="vlm-m-label">Hardware Partition</span>
                <span class="vlm-m-val">${smCount} (${gpuMem})</span>
              </div>
              <div class="vlm-m-item">
                <span class="vlm-m-label">Avg / P95 Latency</span>
                <span class="vlm-m-val">${s.avg_latency_ms ? s.avg_latency_ms + 'ms' : '—'} / ${s.p95_latency_ms ? s.p95_latency_ms + 'ms' : '—'}</span>
              </div>
              <div class="vlm-m-item">
                <span class="vlm-m-label">Completed / Errs</span>
                <span class="vlm-m-val" style="color: ${s.errors > 0 ? '#ef4444' : 'var(--text)'};">${(s.completed || 0).toLocaleString()} / ${(s.errors || 0).toLocaleString()}</span>
              </div>
            </div>
          </div>
        `;
      }).join('');
    }

    // Render Docker table
    const dockerTbody = document.getElementById('vlm-docker-tbody');
    if (dockerTbody) {
      dockerTbody.innerHTML = shards.map((s, idx) => {
        const port = s.port || (s.url ? s.url.split(':').pop() : idx);
        const cName = s.container_name || `rapidalert_vllm_${port === '8000' ? 0 : 1}`;
        const dInfo = s.container || {};
        const dStatus = dInfo.status || (s.healthy ? 'Up (Running)' : 'Warming up');
        const isUp = dStatus.toLowerCase().startsWith('up');
        const smInfo = s.sm_count ? `${s.sm_count} SMs (MIG ${s.mig_profile || 'slice'})` : 'Shared Full GPU';
        const modelImg = dInfo.image || 'vllm/vllm-openai:latest';

        return `
          <tr>
            <td style="font-weight: 600;">🐳 ${this._esc(cName)}</td>
            <td>
              <span class="badge ${isUp ? 'badge-green' : 'badge-amber'}" style="font-size: 0.68rem;">
                ${this._esc(dStatus)}
              </span>
            </td>
            <td>${port} (HTTP)</td>
            <td>${smInfo}</td>
            <td style="color: var(--text-muted); font-size: 0.7rem;">${this._esc(modelImg)}</td>
          </tr>
        `;
      }).join('');
    }
  },

  // ════════════════════════════════════════════════════════════
  //  Prompts
  // ════════════════════════════════════════════════════════════
  _applyPrompts(p) {
    this.prompts = p;
    const masterSceneTA = document.getElementById('master-scene-context-ta');
    const masterTA = document.getElementById('master-prompt-ta');
    const followupTA = document.getElementById('followup-prompt-ta');
    if (masterSceneTA && masterSceneTA !== document.activeElement) masterSceneTA.value = p.master_scene_context || '';
    if (masterTA && masterTA !== document.activeElement) masterTA.value = p.master || '';
    if (followupTA && followupTA !== document.activeElement) followupTA.value = p.followup || '';
    this._updatePromptUnsavedIndicators();
  },

  _updatePromptUnsavedIndicators() {
    const masterTA = document.getElementById('master-prompt-ta');
    const followupTA = document.getElementById('followup-prompt-ta');
    const masterBadge = document.getElementById('master-unsaved-badge');
    const followupBadge = document.getElementById('followup-unsaved-badge');
    
    if (masterTA && masterBadge) {
      masterBadge.style.display = (masterTA.value !== (this.prompts.master || '')) ? 'inline-block' : 'none';
    }
    if (followupTA && followupBadge) {
      followupBadge.style.display = (followupTA.value !== (this.prompts.followup || '')) ? 'inline-block' : 'none';
    }
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

      // Populate Normal Context textareas (day / night) & Incident Rules
      const camCfgCtx = cam.config || {};
      const ctx = document.getElementById('modal-cam-ctx');
      const nightToggle = document.getElementById('modal-cam-night-enabled');
      const ctxNight = document.getElementById('modal-cam-ctx-night');
      const inpSevere = document.getElementById('modal-cam-severe');
      const inpLow = document.getElementById('modal-cam-low');

      if (ctx) ctx.value = camCfgCtx.normal_context || '';
      if (nightToggle) {
        nightToggle.checked = !!camCfgCtx.night_context_enabled;
        ctxNight.style.display = nightToggle.checked ? 'block' : 'none';
      }
      if (ctxNight) ctxNight.value = camCfgCtx.night_context || '';

      const formatRuleText = (val) => {
        if (!val) return '';
        if (Array.isArray(val)) return val.join('\n');
        return String(val);
      };

      if (inpSevere) inpSevere.value = formatRuleText(camCfgCtx.severe_incidents);
      if (inpLow) inpLow.value = formatRuleText(camCfgCtx.low_incidents);

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
      // Compliance is deliberate: no VLM call until the user presses Check compliance.
      this.modalAuditResult = null;
      this.modalAuditState = 'DIRTY';
      this._bindCamContextAuditListeners(name);
      const auditBadge = document.getElementById('modal-cam-audit-status-badge');
      const auditSummary = document.getElementById('modal-cam-audit-summary');
      const auditButton = document.querySelector('#btn-modal-save-context span');
      if (auditBadge) { auditBadge.textContent = '○ Not checked'; auditBadge.className = 'badge badge-muted'; }
      if (auditSummary) { auditSummary.textContent = 'Review the three fields, then press Check compliance. No AI request has been made.'; auditSummary.style.color = 'var(--text-2)'; }
      if (auditButton) auditButton.textContent = '🔍 Check compliance';;
    } catch (err) {
      console.error('Error populating cam modal:', err);
    }

    backdrop.hidden = false;
    backdrop.removeAttribute('hidden');
    backdrop.style.display = 'block';

    modal.hidden = false;
    modal.removeAttribute('hidden');
    modal.style.display = 'flex';
  },

  _bindCamContextAuditListeners(name) {
    const fields = ['modal-cam-severe', 'modal-cam-low', 'modal-cam-ctx', 'modal-cam-ctx-night', 'modal-cam-night-enabled'];
    fields.forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      el.oninput = () => this._onCamContextFieldChanged(name);
      el.onchange = () => this._onCamContextFieldChanged(name);
    });
  },

  _onCamContextFieldChanged(name) {
    this.modalAuditState = 'DIRTY';
    const badge = document.getElementById('modal-cam-audit-status-badge');
    const summary = document.getElementById('modal-cam-audit-summary');
    if (badge) {
      badge.textContent = '● Unsaved Changes (Auditing...)';
      badge.className = 'badge badge-amber';
    }
    this.modalAuditResult = null;
    const actionButton = document.querySelector('#btn-modal-save-context span');
    if (actionButton) actionButton.textContent = '🔍 Check compliance';
    if (summary) summary.textContent = 'Changes detected. Press Check compliance to run one AI audit.';
  },

  async _runCamContextAudit(name, immediate = false) {
    if (!name || name !== this.activeCamModal) return { valid: true };
    const severeText = document.getElementById('modal-cam-severe')?.value || '';
    const lowText = document.getElementById('modal-cam-low')?.value || '';
    const ctx = document.getElementById('modal-cam-ctx')?.value || '';
    const nightEnabled = document.getElementById('modal-cam-night-enabled')?.checked || false;
    const ctxNight = document.getElementById('modal-cam-ctx-night')?.value || '';

    const badge = document.getElementById('modal-cam-audit-status-badge');
    const summary = document.getElementById('modal-cam-audit-summary');
    const conflictsEl = document.getElementById('modal-cam-audit-conflicts');
    const rawBox = document.getElementById('modal-cam-audit-raw-box');
    const rawEl = document.getElementById('modal-cam-audit-raw');

    this.modalAuditState = 'VALIDATING';
    if (badge) {
      badge.textContent = '⏳ AI Auditing Checklists...';
      badge.className = 'badge badge-blue';
    }

    try {
      const res = await this._authFetch('/api/cameras/validate-context', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name,
          severe_text: severeText,
          low_text: lowText,
          normal_context: ctx,
          night_context: ctxNight,
          night_context_enabled: nightEnabled,
        }),
      });

      if (!res.ok) throw new Error('Validation request failed');
      const data = await res.json();
      this.modalAuditResult = data;
      if (rawEl) rawEl.textContent = data.raw_vlm_response || '(No raw VLM response; heuristic fallback was used.)';
      if (rawBox) rawBox.style.display = data.raw_vlm_response ? 'block' : 'none';

      if (data.valid) {
        this.modalAuditState = 'APPROVED';
        if (badge) {
          badge.textContent = '✓ Checklists Harmonized';
          badge.className = 'badge badge-green';
        }
        if (summary) {
          summary.textContent = data.summary || 'No overlapping or contradictory conditions detected across all 3 checklists.';
          summary.style.color = 'var(--text-2)';
        }
        if (conflictsEl) {
          conflictsEl.style.display = 'none';
          conflictsEl.innerHTML = '';
        }
      } else {
        this.modalAuditState = 'CONFLICT';
        if (badge) {
          badge.textContent = '🚨 Contradiction Detected';
          badge.className = 'badge badge-red';
        }
        if (summary) {
          summary.textContent = data.summary || 'Contradictions or overlaps detected between checklists.';
          summary.style.color = '#ef4444';
        }
        if (conflictsEl) {
          conflictsEl.style.display = 'flex';
          const items = (data.conflicts || []).map(c => `
            <div style="background: rgba(239, 68, 68, 0.12); border-left: 3px solid #ef4444; padding: 6px 10px; border-radius: 4px;">
              <strong style="color: #ef4444;">⚠️ [${this._esc(c.type || (Array.isArray(c.fields) ? c.fields.join(' ↔ ') : 'Logical contradiction'))}]:</strong>
              ${this._esc(c.reason || c.issue || '')}
              ${c.severe_item ? `<div style="margin-top: 3px;"><strong>Severe:</strong> ${this._esc(c.severe_item)}</div>` : ''}
              ${c.related_item ? `<div><strong>Related:</strong> ${this._esc(c.related_item)}</div>` : ''}
              ${c.suggestion ? `<div style="margin-top: 3px; color: var(--text); opacity: 0.9;">💡 <em>Recommendation:</em> ${this._esc(c.suggestion)}</div>` : ''}
            </div>
          `).join('');
          conflictsEl.innerHTML = items || `<div style="color: #ef4444;">Contradictions detected. Please review checklists before saving.</div>`;
        }
      }
      return data;
    } catch (err) {
      this.modalAuditState = 'ERROR';
      this.modalAuditResult = { valid: false, unavailable: true };
      if (badge) {
        badge.textContent = '⚠ Audit Unavailable';
        badge.className = 'badge badge-amber';
      }
      if (summary) {
        summary.textContent = 'The AI audit could not be completed. Saving is disabled until validation succeeds.';
        summary.style.color = '#f59e0b';
      }
      return { valid: false, unavailable: true };
    }
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
        const streamSrc = `/api/cameras/${encodeURIComponent(name)}/stream`;
        img.src = streamSrc;
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
      const _pf = topResult.priority_flags && topResult.priority_flags !== '[]' && topResult.priority_flags !== '' ? topResult.priority_flags : null;
        const _rf = topResult.routine_flags  && topResult.routine_flags  !== '[]' && topResult.routine_flags  !== '' ? topResult.routine_flags  : null;
        metaEl.innerHTML = `
        ${_pf ? `<span style="color:var(--red,#ef4444);font-weight:600">🚨 Flagged: ${this._esc(_pf)}</span>` : ''}
        ${_rf ? `<span style="opacity:0.75">✅ Routine: ${this._esc(_rf)}</span>` : ''}
        <span>Model: ${this._esc(topResult.model || 'Cosmos-Nemotron')}</span>
        ${topResult.machinery && topResult.machinery !== 'None' ? `<span>Machinery: ${this._esc(topResult.machinery)}</span>` : ''}
        ${topResult.evolution && topResult.evolution !== 'None' ? `<span>Evolution: ${this._esc(topResult.evolution)}</span>` : ''}
        ${topResult.reasoning && topResult.reasoning !== 'None' ? `<span>Reasoning: ${this._esc(topResult.reasoning)}</span>` : ''}
        <span>Timestamp: ${topResult.ts ? new Date(topResult.ts * 1000).toLocaleString() : '—'}</span>
      `;
    }
  },

  _openModal(name) {
    this._openCamModal(name);
  },

  _closeModal() {
    this.activeCamModal = null;
    const backdrop = document.getElementById('modal-backdrop');
    if (backdrop) {
      backdrop.hidden = true;
      backdrop.setAttribute('hidden', '');
      backdrop.style.display = 'none';
    }
    const modal = document.getElementById('cam-modal');
    if (modal) {
      modal.hidden = true;
      modal.setAttribute('hidden', '');
      modal.style.display = 'none';
    }
    const img = document.getElementById('modal-frame');
    if (img && img.src.startsWith('blob:')) URL.revokeObjectURL(img.src);
    if (img) {
      img.src = '';
      img.removeAttribute('src');
    }
    // Reconnect ONLY active page cards cleanly
    this._renderCameraGrid(true);
  },

  // ════════════════════════════════════════════════════════════
  //  Settings Panel
  // ════════════════════════════════════════════════════════════
  // ════════════════════════════════════════════════════════════
  //  Authenticated API Helper
  // ════════════════════════════════════════════════════════════
  async _authFetch(url, options = {}) {
    const opts = { ...options };
    opts.headers = { ...(opts.headers || {}) };
    if (this.adminToken) {
      opts.headers['Authorization'] = `Bearer ${this.adminToken}`;
      opts.headers['X-Admin-Token'] = this.adminToken;
    }
    const res = await fetch(url, opts);
    if (res.status === 401) {
      this.adminToken = null;
      sessionStorage.removeItem('rapidalert_admin_token');
      sessionStorage.removeItem('rapidalert_admin_user');
      this._updateAuthBadge(false);
      this._closeSettings();
      this._openAuthModal('Administrator credentials required to perform this action.');
    }
    return res;
  },

  // ════════════════════════════════════════════════════════════
  //  Settings Panel & Security Lifecycle
  // ════════════════════════════════════════════════════════════
  async _openSettings() {
    if (this.adminToken) {
      try {
        const res = await fetch('/api/auth/status', {
          headers: { 'Authorization': `Bearer ${this.adminToken}` }
        });
        if (res.ok) {
          const data = await res.json();
          if (data.authenticated) {
            this.adminUser = data.username || this.adminUser;
            this._showSettingsPanel();
            return;
          }
        }
      } catch (e) {
        console.warn('Auth status check failed:', e);
      }
    }
    this._openAuthModal(null, () => {
      this._showSettingsPanel();
    });
  },

  _showSettingsPanel() {
    this._updateAuthBadge(true);
    const backdrop = document.getElementById('settings-backdrop');
    const panel = document.getElementById('settings-panel');
    if (backdrop) {
      backdrop.removeAttribute('hidden');
      backdrop.style.display = 'block';
    }
    if (panel) {
      panel.removeAttribute('hidden');
      panel.style.display = 'flex';
    }
    // Sync cam table
    this._syncCamTable(Object.values(this.cameras).map(c => c.config));
    // Sync master scene context
    const masterSceneTA = document.getElementById('master-scene-context-ta');
    if (masterSceneTA) masterSceneTA.value = this.prompts.master_scene_context || '';
    // Sync master prompt
    const masterTA = document.getElementById('master-prompt-ta');
    if (masterTA) masterTA.value = this.prompts.master || '';
    // Sync followup prompt
    const followupTA = document.getElementById('followup-prompt-ta');
    if (followupTA) followupTA.value = this.prompts.followup || '';
    // Sync VLM info
    this._fetchVLMEndpoints();
  },

  _updateAuthBadge(isAuth) {
    const badge = document.getElementById('settings-auth-badge');
    const lockBtn = document.getElementById('btn-lock-settings');
    const sessInfo = document.getElementById('security-session-info');
    if (badge) {
      badge.textContent = isAuth ? `🔒 ${this.adminUser || 'Admin'}` : '🔒 Locked';
      badge.className = isAuth ? 'badge badge-green badge-sm' : 'badge badge-muted badge-sm';
    }
    if (lockBtn) {
      lockBtn.style.display = isAuth ? 'inline-flex' : 'none';
    }
    if (sessInfo) {
      sessInfo.textContent = isAuth ? `Logged in as ${this.adminUser || 'admin'} (PBKDF2-HMAC-SHA256)` : 'Session locked';
    }
  },

  async _openAuthModal(errMsg = null, onSuccess = null) {
    this._authSuccessCallback = onSuccess;
    const modal = document.getElementById('modal-admin-auth');
    if (!modal) return;
    modal.style.display = 'flex';
    const errBanner = document.getElementById('auth-error-banner');
    const errMsgEl = document.getElementById('auth-error-msg');
    const pwInput = document.getElementById('auth-input-password');
    const userInput = document.getElementById('auth-input-username');
    if (pwInput) pwInput.value = '';
    if (userInput && !userInput.value) userInput.value = 'admin';

    if (errMsg && errBanner && errMsgEl) {
      errMsgEl.textContent = errMsg;
      errBanner.style.display = 'flex';
    } else if (errBanner) {
      errBanner.style.display = 'none';
    }

    try {
      const res = await fetch('/api/auth/status');
      if (res.ok) {
        const data = await res.json();
        this.isAuthConfigured = data.configured !== false;
        const title = document.getElementById('auth-modal-title');
        const sub = document.getElementById('auth-modal-sub');
        const btnLabel = document.getElementById('btn-auth-label');
        if (!this.isAuthConfigured) {
          if (title) title.textContent = 'Setup Master Admin';
          if (sub) sub.textContent = 'Create your initial administrator credentials';
          if (btnLabel) btnLabel.textContent = 'Create Master Password';
        } else {
          if (title) title.textContent = 'Admin Unlock';
          if (sub) sub.textContent = 'Encrypted configuration control';
          if (btnLabel) btnLabel.textContent = '🔓 Unlock';
        }
      }
    } catch (e) {}

    setTimeout(() => { if (pwInput) pwInput.focus(); }, 50);
  },

  _closeAuthModal() {
    this._authSuccessCallback = null;
    const modal = document.getElementById('modal-admin-auth');
    if (modal) modal.style.display = 'none';
    this._renderCameraGrid();
  },

  async _submitAuth() {
    const userInput = document.getElementById('auth-input-username');
    const pwInput = document.getElementById('auth-input-password');
    const errBanner = document.getElementById('auth-error-banner');
    const errMsgEl = document.getElementById('auth-error-msg');
    const btnLabel = document.getElementById('btn-auth-label');

    const username = (userInput?.value || 'admin').trim();
    const password = pwInput?.value || '';

    if (!password) {
      if (errMsgEl) errMsgEl.textContent = 'Please enter password.';
      if (errBanner) errBanner.style.display = 'flex';
      return;
    }

    const endpoint = this.isAuthConfigured ? '/api/auth/login' : '/api/auth/setup';
    const submitBtn = document.getElementById('btn-submit-auth');
    const originalText = btnLabel ? btnLabel.textContent : 'Unlock';
    if (btnLabel) btnLabel.textContent = 'Verifying...';
    if (submitBtn) submitBtn.disabled = true;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 6000);

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      const data = await res.json();
      if (res.ok && data.token) {
        this.adminToken = data.token;
        this.adminUser = data.username || username;
        this.isAuthConfigured = true;
        sessionStorage.setItem('rapidalert_admin_token', data.token);
        sessionStorage.setItem('rapidalert_admin_user', this.adminUser);

        // Capture callback before closing modal
        const successCb = this._authSuccessCallback;
        this._authSuccessCallback = null;

        const modal = document.getElementById('modal-admin-auth');
        if (modal) modal.style.display = 'none';

        this._showToast('🔓 Administrator credentials verified.', 'ok');
        
        if (typeof successCb === 'function') {
          successCb();
        } else {
          this._showSettingsPanel();
        }
      } else {
        if (errMsgEl) errMsgEl.textContent = data.detail || 'Invalid username or password.';
        if (errBanner) errBanner.style.display = 'flex';
        if (pwInput) {
          pwInput.value = '';
          pwInput.focus();
        }
      }
    } catch (e) {
      clearTimeout(timeoutId);
      if (errMsgEl) errMsgEl.textContent = e.name === 'AbortError' ? 'Authentication timed out. Please try again.' : 'Network or server error during authentication.';
      if (errBanner) errBanner.style.display = 'flex';
    } finally {
      if (btnLabel) btnLabel.textContent = originalText;
      if (submitBtn) submitBtn.disabled = false;
    }
  },

  async _lockSettings() {
    if (this.adminToken) {
      fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${this.adminToken}` }
      }).catch(() => {});
    }
    this.adminToken = null;
    sessionStorage.removeItem('rapidalert_admin_token');
    sessionStorage.removeItem('rapidalert_admin_user');
    this._updateAuthBadge(false);
    this._closeSettings();
    this._showToast('🔒 Settings locked. Admin session terminated.', 'ok');
  },

  async _submitPasswordChange() {
    const oldPw = document.getElementById('input-old-pw')?.value || '';
    const newPw = document.getElementById('input-new-pw')?.value || '';
    const confirmPw = document.getElementById('input-confirm-pw')?.value || '';
    const alertEl = document.getElementById('pw-change-alert');

    const showAlert = (msg, isErr) => {
      if (!alertEl) return;
      alertEl.style.display = 'block';
      alertEl.textContent = msg;
      alertEl.style.background = isErr ? 'rgba(239, 68, 68, 0.15)' : 'rgba(16, 185, 129, 0.15)';
      alertEl.style.border = isErr ? '1px solid rgba(239, 68, 68, 0.4)' : '1px solid rgba(16, 185, 129, 0.4)';
      alertEl.style.color = isErr ? '#f87171' : '#34d399';
    };

    if (!oldPw) return showAlert('Please enter your current password', true);
    if (!newPw || newPw.length < 4) return showAlert('New password must be at least 4 characters', true);
    if (newPw !== confirmPw) return showAlert('New passwords do not match', true);

    try {
      const res = await this._authFetch('/api/auth/change-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ old_password: oldPw, new_password: newPw })
      });
      const data = await res.json();
      if (res.ok) {
        showAlert('✅ Master password successfully updated and encrypted in database.', false);
        const elOld = document.getElementById('input-old-pw');
        const elNew = document.getElementById('input-new-pw');
        const elConf = document.getElementById('input-confirm-pw');
        if (elOld) elOld.value = '';
        if (elNew) elNew.value = '';
        if (elConf) elConf.value = '';
        this._showToast('Master password successfully updated', 'ok');
      } else {
        showAlert(data.detail || 'Failed to update password', true);
      }
    } catch (e) {
      showAlert('Error updating password', true);
    }
  },

  _closeSettings() {
    const backdrop = document.getElementById('settings-backdrop');
    const panel = document.getElementById('settings-panel');
    if (backdrop) {
      backdrop.setAttribute('hidden', '');
      backdrop.style.display = 'none';
    }
    if (panel) {
      panel.setAttribute('hidden', '');
      panel.style.display = 'none';
    }

    // Auto-lock admin session every time Settings is closed
    if (this.adminToken) {
      fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${this.adminToken}` }
      }).catch(() => {});
    }
    this.adminToken = null;
    sessionStorage.removeItem('rapidalert_admin_token');
    sessionStorage.removeItem('rapidalert_admin_user');
    this._updateAuthBadge(false);
    this._renderCameraGrid();
  },

  // ════════════════════════════════════════════════════════════
  //  Manual Test Incident Trigger Dialog
  // ════════════════════════════════════════════════════════════
  _openTestTriggerModal() {
    if (!this.adminToken) {
      this._openAuthModal('Administrator credentials required to trigger synthetic incident alert.', () => {
        this._openTestTriggerModal();
      });
      return;
    }

    const select = document.getElementById('test-trigger-cam-select');
    if (select) {
      const camNames = Object.keys(this.cameras || {});
      if (camNames.length > 0) {
        select.innerHTML = camNames.map(name => {
          const cam = this.cameras[name];
          const isEnabled = cam?.config?.enabled !== false;
          return `<option value="${this._esc(name)}">${this._esc(name)} ${isEnabled ? '(Active)' : '(Offline)'}</option>`;
        }).join('');
      } else {
        select.innerHTML = '<option value="">No cameras configured</option>';
      }
    }

    const modal = document.getElementById('modal-test-trigger');
    if (modal) modal.style.display = 'flex';
  },

  _closeTestTriggerModal() {
    const modal = document.getElementById('modal-test-trigger');
    if (modal) modal.style.display = 'none';

    // Auto-lock session after closing test trigger popup
    if (this.adminToken) {
      fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${this.adminToken}` }
      }).catch(() => {});
    }
    this.adminToken = null;
    sessionStorage.removeItem('rapidalert_admin_token');
    sessionStorage.removeItem('rapidalert_admin_user');
    this._updateAuthBadge(false);
  },

  async _submitTestTrigger() {
    const camSelect = document.getElementById('test-trigger-cam-select');
    const sevSelect = document.getElementById('test-trigger-sev-select');
    const modeSelect = document.getElementById('test-trigger-mode-select');

    const cam = camSelect?.value;
    const severity = sevSelect?.value || 'HIGH';
    const followup = modeSelect?.value === 'followup';

    if (!cam) {
      this._showToast('Please select a camera feed first.', 'err');
      return;
    }

    const btn = document.getElementById('btn-submit-test-trigger');
    if (btn) btn.disabled = true;

    try {
      const url = `/api/alerts/test?cam=${encodeURIComponent(cam)}&severity=${encodeURIComponent(severity)}&followup=${followup}`;
      const res = await this._authFetch(url, { method: 'POST' });
      if (res.ok) {
        const body = await res.json();
        this._showToast(`⚡ Dispatched test incident for ${cam} (${severity})!`, 'ok');
        if (body && body.alert) {
          this.onAlert({ alert: body.alert });
        }
        this._closeTestTriggerModal();
      } else {
        this._showToast('Failed to trigger test alert (Admin authentication required)', 'err');
      }
    } catch (e) {
      this._showToast(`Error: ${e.message}`, 'err');
    } finally {
      if (btn) btn.disabled = false;
    }
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
        <td><input type="text" class="input ctx" data-cam="${this._esc(cam.name)}"
                   value="${this._esc(cam.normal_context || '')}" placeholder="Routine context…"></td>
        <td><button class="btn-danger btn-del" data-cam="${this._esc(cam.name)}">🗑</button></td>
      `;
      tbody.appendChild(tr);
    }
  },

  // ════════════════════════════════════════════════════════════
  //  API Calls
  // ════════════════════════════════════════════════════════════
  async _apiUpsertCamera(cam) {
    await this._authFetch('/api/cameras', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(cam),
    });
  },

  async _apiDeleteCamera(name) {
    await this._authFetch(`/api/cameras/${encodeURIComponent(name)}`, { method: 'DELETE' });
  },

  async _saveMasterSceneContext() {
    const text = document.getElementById('master-scene-context-ta')?.value;
    if (text == null) return;
    await this._authFetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ master_scene_context: text }),
    });
    this._flashSaveFeedback('master-scene-save-fb', '✓ Saved');
  },

  async _saveMasterPrompt() {
    const text = document.getElementById('master-prompt-ta')?.value;
    if (text == null) return;
    await this._authFetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ master: text }),
    });
    this._flashSaveFeedback('master-save-fb', '✓ Saved');
  },

  async _saveFollowupPrompt() {
    const text = document.getElementById('followup-prompt-ta')?.value;
    if (text == null) return;
    await this._authFetch('/api/prompts', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ followup: text }),
    });
    this._flashSaveFeedback('followup-save-fb', '✓ Saved');
  },

  async _saveModalContext() {
    const name = this.activeCamModal;
    if (!name) return;
    const ctx = document.getElementById('modal-cam-ctx')?.value ?? '';
    const nightEnabled = document.getElementById('modal-cam-night-enabled')?.checked ?? false;
    const ctxNight = document.getElementById('modal-cam-ctx-night')?.value ?? '';
    const fb = document.getElementById('modal-context-fb');
    const cam = this.cameras[name];
    if (!cam) return;
    const cfg = { ...(cam.config || {}), name, normal_context: ctx, night_context_enabled: nightEnabled, night_context: ctxNight };
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
    const ctx = document.getElementById('new-cam-ctx')?.value.trim() || '';
    if (!name || !url) { this._showToast('Enter a name and RTSP URL', 'warn'); return; }
    await this._apiUpsertCamera({ name, url, enabled: true, normal_context: ctx });
    document.getElementById('new-cam-name').value = '';
    document.getElementById('new-cam-url').value  = '';
    if (document.getElementById('new-cam-ctx')) document.getElementById('new-cam-ctx').value = '';
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
        this._updateVlmShardsUI(statsData);
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
        const frameAgo = cam.lastFrameTs ? (now - cam.lastFrameTs) : (cam.lastTs ? (now - cam.lastTs) : 9999);
        const analysisAgo = cam.lastAnalysisTs ? (now - cam.lastAnalysisTs) : (cam.lastTs ? (now - cam.lastTs) : null);

        const el = document.getElementById(`ts-${this._eid(name)}`);
        if (el) {
          if (frameAgo < 10) {
            el.textContent = 'Live';
          } else if (analysisAgo !== null) {
            el.textContent = analysisAgo < 60
              ? `${Math.round(analysisAgo)}s ago`
              : analysisAgo < 3600
                ? `${Math.round(analysisAgo / 60)}m ago`
                : `${Math.round(analysisAgo / 3600)}h ago`;
          } else {
            el.textContent = 'Connecting…';
          }
        }

        // Live dot colour based on real-time frame arrival
        const dot = document.getElementById(`dot-${this._eid(name)}`);
        if (dot) {
          if      (frameAgo < 15)  dot.className = 'cam-live-dot live-ok';
          else if (frameAgo < 45)  dot.className = 'cam-live-dot live-warn';
          else                     dot.className = 'cam-live-dot';
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
      const r = await this._authFetch('/api/scanner/scan', {
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
    
    // Count total channels across all discovered devices
    let totalChannels = 0;
    for (const dev of found) {
      if (dev.channels && dev.channels.length > 0) {
        totalChannels += dev.channels.length;
      } else if (dev.rtsp_urls) {
        totalChannels += dev.rtsp_urls.length;
      }
    }

    count.textContent = `${totalChannels} camera channel${totalChannels !== 1 ? 's' : ''} found across ${found.length} device${found.length !== 1 ? 's' : ''}`;
    list.innerHTML = '';

    if (found.length === 0 || totalChannels === 0) {
      list.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-3);font-size:0.85rem;">No verified RTSP cameras found. Please check your subnet / IP range or enter credentials.</div>';
      return;
    }

    for (const dev of found) {
      const vendor = dev.vendor || 'IP Camera';
      const model = dev.model || '';
      const ip = dev.ip;
      const port = dev.port || 554;
      const channels = dev.channels || (dev.rtsp_urls || []).map((u, i) => ({
        channel: i + 1,
        name: `CAM_${ip.replace(/\./g, '_')}_CH${i + 1}`,
        rtsp_url: u,
        codec: 'H.264',
        verified: true,
      }));

      const item = document.createElement('div');
      item.className = 'scan-item';
      item.style.padding = '14px';
      item.style.borderBottom = '1px solid var(--border)';
      item.style.display = 'flex';
      item.style.flexDirection = 'column';
      item.style.gap = '10px';

      const channelsHtml = channels.map(ch => {
        const chName = ch.name || `Channel ${ch.channel}`;
        const chUrl = ch.rtsp_url;
        const codec = ch.codec || 'H.264';
        return `
          <div class="scan-url-row" style="display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 6px 10px; background: var(--bg-card-hover, rgba(255,255,255,0.03)); border-radius: var(--radius-sm, 6px); border: 1px solid var(--border);">
            <div style="display: flex; align-items: center; gap: 8px; flex: 1; min-width: 0;">
              <span class="badge" style="font-size: 0.65rem; font-family: var(--font-mono); padding: 2px 6px; background: var(--accent-dim, rgba(0,200,255,0.15)); color: var(--accent, #00c8ff); font-weight: 700;">CH ${ch.channel}</span>
              <span style="font-weight: 600; font-size: 0.8rem; color: var(--text, #fff); white-space: nowrap;">${this._esc(chName)}</span>
              <span class="badge" style="font-size: 0.62rem; padding: 1px 5px; background: rgba(255,255,255,0.08); color: var(--text-2, #aaa);">${this._esc(codec)}</span>
              <span class="scan-url-text" title="${this._esc(chUrl)}" style="font-size: 0.68rem; color: var(--text-3, #777); font-family: var(--font-mono); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1;">${this._esc(chUrl)}</span>
            </div>
            <button class="btn-use-url" data-url="${this._esc(chUrl)}" data-name="${this._esc(chName)}" data-ip="${this._esc(ip)}" style="padding: 4px 10px; font-size: 0.72rem; font-weight: 600; background: var(--accent, #00c8ff); color: #000; border: none; border-radius: 4px; cursor: pointer; flex-shrink: 0;">+ Use Camera</button>
          </div>
        `;
      }).join('');

      item.innerHTML = `
        <div class="scan-item-header" style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px;">
          <div style="display: flex; align-items: center; gap: 8px;">
            <span style="font-size: 1rem;">📹</span>
            <div>
              <span style="font-weight: 700; font-size: 0.9rem; color: var(--text, #fff);">${this._esc(vendor)}</span>
              ${model ? `<span style="font-size: 0.72rem; color: var(--text-3, #888); margin-left: 6px;">(${this._esc(model)})</span>` : ''}
            </div>
          </div>
          <div style="display: flex; align-items: center; gap: 8px;">
            <span class="scan-ip" style="font-weight: 700; font-family: var(--font-mono); color: var(--cyan, #00e5ff); font-size: 0.82rem;">${this._esc(ip)}:${port}</span>
            <span class="badge badge-green" style="font-size: 0.65rem; padding: 2px 6px;">${channels.length} Live Channel${channels.length !== 1 ? 's' : ''}</span>
          </div>
        </div>
        <div class="scan-urls" style="display: flex; flex-direction: column; gap: 6px;">${channelsHtml}</div>
      `;
      list.appendChild(item);
    }

    // Click handler for "Use Camera" buttons
    list.querySelectorAll('.btn-use-url').forEach(btn => {
      btn.addEventListener('click', () => {
        const url = btn.dataset.url;
        const name = btn.dataset.name || `CAM_${btn.dataset.ip.replace(/\./g, '_')}`;
        
        // Auto-fill camera add form and switch to cameras tab
        const nameEl = document.getElementById('new-cam-name');
        const urlEl  = document.getElementById('new-cam-url');
        if (nameEl) nameEl.value = name.replace(/\s+/g, '_').toUpperCase();
        if (urlEl)  urlEl.value  = url;
        
        // Switch to cameras tab
        document.querySelectorAll('.stab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.stab-content').forEach(c => c.classList.remove('active'));
        const camTab = document.querySelector('.stab[data-stab="cameras"]');
        if (camTab) camTab.classList.add('active');
        document.getElementById('stab-cameras')?.classList.add('active');
        this._toast(`Selected camera "${name}" → Cameras tab ready to add!`);
      });
    });
  },

  // ════════════════════════════════════════════════════════════
  //  Shift Reporting & Automated PDF Delivery
  // ════════════════════════════════════════════════════════════
  _cachedReports: [],
  _reportFilter: 'all',

  _openReportsModal() {
    const backdrop = document.getElementById('reports-modal-backdrop');
    const modal = document.getElementById('modal-reports-explorer');
    if (backdrop) backdrop.hidden = false;
    if (modal) modal.hidden = false;
    this._fetchReports();
    this._fetchReportingStats();
  },

  _closeReportsModal() {
    const backdrop = document.getElementById('reports-modal-backdrop');
    const modal = document.getElementById('modal-reports-explorer');
    if (backdrop) backdrop.hidden = true;
    if (modal) modal.hidden = true;
  },

  async _fetchReports() {
    try {
      const res = await fetch('/api/reports?limit=100');
      if (!res.ok) return;
      const reports = await res.json();
      this._cachedReports = reports || [];

      // Update Header badge count
      const headerCountEl = document.getElementById('header-reports-count');
      if (headerCountEl) {
        headerCountEl.textContent = this._cachedReports.length;
      }

      // Render into settings tab table if present
      const tbody = document.getElementById('reports-table-body');
      if (tbody) {
        if (!this._cachedReports.length) {
          tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-3); padding: 24px;">No shift reports generated yet. Click "Generate & Email Report Now" or wait for 06:00 AM / 06:00 PM scheduled slots.</td></tr>';
        } else {
          tbody.innerHTML = this._cachedReports.map(r => {
            const stats = r.summary?.stats || {};
            const isEmailSent = r.email_status === 'SENT';
            const emailBadge = isEmailSent
              ? `<span class="badge badge-green badge-sm">✅ Dispatched</span>`
              : (r.email_status === 'FAILED' ? `<span class="badge badge-red badge-sm" title="${this._esc(r.error_message || '')}">❌ Failed</span>` : `<span class="badge badge-muted badge-sm">${r.email_status}</span>`);
            
            const isNight = (r.shift_type || '').toUpperCase().includes('NIGHT');
            const shiftBadge = isNight
              ? `<span class="badge badge-muted badge-sm">🌙 NIGHT (${r.time_frame || '18:00 - 06:00'})</span>`
              : `<span class="badge badge-green badge-sm">☀️ DAY (${r.time_frame || '06:00 - 18:00'})</span>`;

            return `
              <tr>
                <td>
                  <div style="font-weight: 600; color: var(--text-1);">${r.generated_at_str}</div>
                  <div style="font-size: 0.72rem; color: var(--text-3); font-family: var(--font-mono);">${this._esc(r.pdf_name)} (${r.file_size_kb || 0} KB)</div>
                </td>
                <td>${shiftBadge}</td>
                <td>
                  <span style="font-weight: 600;">${stats.total_analyses || 0}</span> analyses
                  ${stats.high_count > 0 ? `<span style="color: #f87171; font-weight: 700; margin-left: 6px;">(${stats.high_count} HIGH)</span>` : '<span style="color: #34d399; margin-left: 6px;">(0 High)</span>'}
                </td>
                <td>
                  ${emailBadge}
                  <div style="font-size: 0.7rem; color: var(--text-3); margin-top: 2px;">${this._esc(r.email_recipients || '')}</div>
                </td>
                <td style="text-align: right;">
                  <a href="/api/reports/${r.id}/pdf" target="_blank" class="btn-ghost" style="padding: 4px 8px; font-size: 0.75rem; text-decoration: none; display: inline-flex; align-items: center; gap: 4px;">
                    📄 View PDF
                  </a>
                </td>
              </tr>
            `;
          }).join('');
        }
      }

      // Render Modal Cards
      this._renderReportsCards();

    } catch (e) {
      console.warn('Failed to fetch reports:', e);
    }
  },

  _renderReportsCards() {
    const container = document.getElementById('reports-cards-container');
    if (!container) return;

    let filtered = this._cachedReports || [];
    if (this._reportFilter === 'DAY') {
      filtered = filtered.filter(r => (r.shift_type || '').toUpperCase().includes('DAY'));
    } else if (this._reportFilter === 'NIGHT') {
      filtered = filtered.filter(r => (r.shift_type || '').toUpperCase().includes('NIGHT'));
    }

    if (!filtered.length) {
      container.innerHTML = `
        <div class="reports-empty-state">
          <div style="font-size: 2.2rem; line-height: 1;">📂</div>
          <div style="font-weight: 600; color: var(--text-secondary);">No shift reports found matching this filter</div>
          <small style="color: var(--text-tertiary);">Automatic executive reports generate at 06:00 AM and 06:00 PM IST (Rolling buffer: 69 max).</small>
        </div>
      `;
      return;
    }

    container.innerHTML = filtered.map(r => {
      const isNight = (r.shift_type || '').toUpperCase().includes('NIGHT');
      const cardClass = isNight ? 'report-card shift-night' : 'report-card shift-day';
      const shiftIcon = isNight ? '🌙' : '☀️';
      const shiftLabel = isNight ? 'NIGHT SHIFT' : 'DAY SHIFT';
      const badgeClass = isNight ? 'report-badge-shift night' : 'report-badge-shift day';

      const stats = r.summary?.stats || {};
      const routines = r.summary?.zone_routines || {};
      const parking = r.summary?.parking_counts || '2W: 0, 4W: 0, HV: 0';

      // Pick a representative routine summary quote
      const routineCams = Object.keys(routines);
      const firstRoutine = routineCams.length > 0 ? `${routineCams[0]}: ${routines[routineCams[0]]}` : 'Standard continuous surveillance executed across all camera zones.';

      const isSent = r.email_status === 'SENT';
      const emailBadge = isSent
        ? `<span class="badge badge-green badge-sm">✅ Dispatched</span>`
        : (r.email_status === 'FAILED' ? `<span class="badge badge-red badge-sm" title="${this._esc(r.error_message || '')}">❌ Delivery Failed</span>` : `<span class="badge badge-muted badge-sm">${r.email_status}</span>`);

      return `
        <div class="${cardClass}">
          <div class="report-card-left">
            <div class="report-card-title">${r.title || 'Site Shift Report'}</div>
            <div class="report-shift-pills">
              <span class="${badgeClass}">${shiftIcon} ${shiftLabel}</span>
              <span class="report-timeframe-badge">⏱️ ${r.time_frame || (isNight ? '18:00 – 06:00 IST' : '06:00 – 18:00 IST')}</span>
            </div>
            <div class="report-gen-time">Generated: <strong>${r.generated_at_str}</strong></div>
          </div>

          <div class="report-card-middle">
            <div class="report-kpi-chips">
              <span class="report-kpi-chip">📊 Analyses: <strong>${stats.total_analyses || 0}</strong></span>
              <span class="report-kpi-chip ${stats.high_count > 0 ? 'kpi-high' : 'kpi-ok'}">🚨 High Severity: <strong>${stats.high_count || 0}</strong></span>
              <span class="report-kpi-chip">⚠️ Medium: <strong>${stats.medium_count || 0}</strong></span>
              <span class="report-kpi-chip">🅿️ Parking: <strong>${this._esc(parking.replace(/\\n/g, ' · '))}</strong></span>
            </div>
            <div class="report-routine-snippet" title="${this._esc(firstRoutine)}">
              🔍 <strong>AI Shift Routine:</strong> ${this._esc(firstRoutine)}
            </div>
          </div>

          <div class="report-card-right">
            <div class="report-status-wrap">
              ${emailBadge}
              <span class="report-file-size">${r.file_size_kb || 0} KB</span>
            </div>
            <div class="report-card-actions">
              <a href="/api/reports/${r.id}/pdf" target="_blank" class="btn btn-sm btn-ghost" title="Open compiled PDF in new browser tab">
                👁️ View PDF
              </a>
              <a href="/api/reports/${r.id}/pdf" download="${r.pdf_name}" class="btn btn-sm btn-primary" title="Download PDF document locally">
                📥 Download
              </a>
            </div>
          </div>
        </div>
      `;
    }).join('');
  },

  async _fetchReportingStats() {
    try {
      const res = await fetch('/api/reports/stats');
      if (res.ok) {
        const d = await res.json();
        
        // Header badge
        const headerCountEl = document.getElementById('header-reports-count');
        if (headerCountEl) headerCountEl.textContent = d.stored_count || 0;

        // Settings tab elements
        const nextEl = document.getElementById('reports-next-run');
        const recipEl = document.getElementById('reports-recipients-list');
        if (nextEl) nextEl.textContent = `Next Run: ${d.next_scheduled_run} (${d.next_shift} Shift)`;
        if (recipEl) recipEl.textContent = (d.recipients || []).join(', ') || 'None';

        // Modal buffer banner & KPI strip
        const bannerEl = document.getElementById('reports-buffer-banner');
        if (bannerEl) {
          bannerEl.textContent = `Rolling Buffer: ${d.stored_count} / ${d.max_buffer} Reports (${d.disk_usage_mb} MB on disk · ~${d.buffer_coverage_days} days retention)`;
        }

        const rmStored = document.getElementById('rm-stored-count');
        if (rmStored) rmStored.textContent = `${d.stored_count} / ${d.max_buffer}`;

        const rmDisk = document.getElementById('rm-disk-usage');
        if (rmDisk) rmDisk.textContent = `${d.disk_usage_mb} MB`;

        const rmCoverage = document.getElementById('rm-coverage-days');
        if (rmCoverage) rmCoverage.textContent = `${d.buffer_coverage_days} Days`;

        const rmSchedule = document.getElementById('rm-schedule-hours');
        if (rmSchedule) rmSchedule.textContent = (d.schedule_hours || [6, 18]).map(h => `${String(h).padStart(2, '0')}:00`).join(' & ') + ' IST';

        const rmNext = document.getElementById('rm-next-run');
        if (rmNext) rmNext.textContent = `${d.next_scheduled_run} (${d.next_shift})`;

        const modalRecip = document.getElementById('reports-modal-recipients');
        if (modalRecip) modalRecip.textContent = `Recipients: ${(d.recipients || []).join(', ') || 'None'}`;
      }
    } catch (e) {}
  },

  async _generateReportNow() {
    const btns = [
      document.getElementById('btn-generate-report-now'),
      document.getElementById('btn-modal-generate-report')
    ].filter(Boolean);

    btns.forEach(b => {
      b.dataset.origText = b.innerHTML;
      b.innerHTML = '<span>⏳ Compiling & Emailing...</span>';
      b.disabled = true;
    });

    this._showToast('📑 Compiling executive shift report & dispatching email...', 'info');

    try {
      const res = await fetch('/api/reports/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ shift_type: 'AUTO', send_email: true })
      });
      const data = await res.json();
      if (res.ok) {
        this._showToast(`✅ Shift report generated (${data.pdf_name}) & email ${data.email_status}!`, 'ok');
        this._fetchReports();
        this._fetchReportingStats();
      } else {
        this._showToast(`Report generation failed: ${data.detail || 'Unknown error'}`, 'danger');
      }
    } catch (e) {
      this._showToast('Network error generating report', 'danger');
    } finally {
      btns.forEach(b => {
        if (b.dataset.origText) b.innerHTML = b.dataset.origText;
        b.disabled = false;
      });
    }
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

    // VLM Container & Shards Inspector open/close
    document.getElementById('vlm-shards-pill')?.addEventListener('click', () => this._openVlmInspector());
    document.getElementById('btn-close-vlm-inspector')?.addEventListener('click', () => this._closeVlmInspector());
    document.getElementById('modal-vlm-inspector')?.addEventListener('click', (e) => {
      if (e.target.id === 'modal-vlm-inspector') this._closeVlmInspector();
    });
    document.getElementById('btn-vlm-probe-now')?.addEventListener('click', () => this._probeVlmShards());

    // Follow-up Observation Banner chip click delegation
    document.getElementById('followup-chips-list')?.addEventListener('click', (e) => {
      const chip = e.target.closest('.followup-flashing-chip');
      if (chip && chip.dataset.cam) {
        this.jumpToCamera(chip.dataset.cam);
      }
    });

    // Centralized Camera Grid Event Delegation (Robust click handling for cards, streams, and event thumbnails)
    const cameraGrid = document.getElementById('camera-grid');
    if (cameraGrid) {
      cameraGrid.addEventListener('click', (e) => {
        // 1. Check if user clicked a specific event thumbnail
        const thumb = e.target.closest('.cam-event-thumb-item');
        if (thumb) {
          e.stopPropagation();
          const camName = thumb.getAttribute('data-cam');
          const idx = parseInt(thumb.getAttribute('data-idx'), 10);
          if (camName) {
            this._openCamModal(camName, isNaN(idx) ? null : idx);
          }
          return;
        }

        // 2. Ignore buttons / inputs / forms
        if (e.target.closest('button') || e.target.closest('input') || e.target.closest('textarea') || e.target.closest('select')) {
          return;
        }

        // 3. Find parent camera card and open Live Stream Theater
        const card = e.target.closest('.cam-card');
        if (card) {
          const camName = card.getAttribute('data-cam') || Object.keys(this.cameras).find(c => `cam-card-${this._eid(c)}` === card.id);
          if (camName) {
            this._openCamModal(camName, null);
          }
        }
      });
    }

    // Alert filter tabs (All, High, Med+, Trigger)
    document.querySelectorAll('.alert-filter-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.alert-filter-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeAlertFilter = btn.dataset.alertFilter || 'all';
        this._renderAllAlerts();
      });
    });

    // Camera filter chips
    document.querySelectorAll('.matrix-filters .filter-chip').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.matrix-filters .filter-chip').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.activeFilter = btn.dataset.filter || 'all';
        this.camCurrentPage = 1;
        this._renderCameraGrid();
      });
    });

    // Grid layout switcher (2x2 vs 3x2)
    document.querySelectorAll('.btn-layout-toggle').forEach(btn => {
      btn.addEventListener('click', () => {
        const cams = parseInt(btn.dataset.cams, 10);
        this.setLayout(cams);
      });
    });

    // Quick Live Stream Resync Button
    document.getElementById('btn-resync-streams')?.addEventListener('click', () => {
      this.resyncAllStreams();
    });

    // Camera pagination navigation (multicam_behavior_test style)
    document.getElementById('btn-cam-prev-page')?.addEventListener('click', () => {
      this.changePage(-1);
    });

    document.getElementById('btn-cam-next-page')?.addEventListener('click', () => {
      this.changePage(1);
    });

    document.getElementById('cam-page-pills')?.addEventListener('click', (e) => {
      const pill = e.target.closest('.cam-page-pill');
      if (pill && pill.dataset.page) {
        const page = parseInt(pill.dataset.page, 10);
        if (!isNaN(page)) this.setCamPage(page);
      }
    });

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
        if (btn.dataset.stab === 'reports') {
          this._fetchReports();
          this._fetchReportingStats();
        }
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
        await this._authFetch('/api/cameras/batch', {
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
        await this._authFetch('/api/cameras/batch', {
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
      if (e.target.classList.contains('ctx'))            cfg.normal_context = e.target.value;
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
    document.getElementById('btn-save-master-scene-context')?.addEventListener('click', () => this._saveMasterSceneContext());
    document.getElementById('btn-save-master')?.addEventListener('click', () => this._saveMasterPrompt());
    document.getElementById('btn-save-followup')?.addEventListener('click', () => this._saveFollowupPrompt());

    document.getElementById('master-prompt-ta')?.addEventListener('input', () => this._updatePromptUnsavedIndicators());
    document.getElementById('followup-prompt-ta')?.addEventListener('input', () => this._updatePromptUnsavedIndicators());

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
        const res = await this._authFetch('/api/config', {
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

    // Theater Modal Save Context & Incident Rules
    document.getElementById('modal-cam-night-enabled')?.addEventListener('change', (e) => {
      const ctxNight = document.getElementById('modal-cam-ctx-night');
      if (ctxNight) ctxNight.style.display = e.target.checked ? 'block' : 'none';
    });

    document.getElementById('btn-modal-save-context')?.addEventListener('click', async () => {
      const name = this.activeCamModal;
      if (!name) return;
      const ctx = document.getElementById('modal-cam-ctx')?.value || '';
      const nightEnabled = document.getElementById('modal-cam-night-enabled')?.checked || false;
      const ctxNight = document.getElementById('modal-cam-ctx-night')?.value || '';
      const severeText = document.getElementById('modal-cam-severe')?.value || '';
      const lowText = document.getElementById('modal-cam-low')?.value || '';
      const fb = document.getElementById('modal-context-fb');

      // First click performs the one deliberate compliance check.
      // Saving is only available after that check has approved the unchanged fields.
      const approved = this.modalAuditState === 'APPROVED' && this.modalAuditResult?.valid === true;
      if (!approved) {
        const auditRes = await this._runCamContextAudit(name, true);
        const actionButton = document.querySelector('#btn-modal-save-context span');
        if (auditRes?.valid === true) {
          if (actionButton) actionButton.textContent = '💾 Save & hot-reload';
          if (fb) {
            fb.textContent = '✅ Compliance approved. Press Save & hot-reload.';
            fb.style.color = 'var(--green)';
          }
        } else {
          if (actionButton) actionButton.textContent = '🔍 Check compliance';
          if (fb) {
            fb.textContent = auditRes?.unavailable ? '⚠ Compliance check unavailable' : '❌ Resolve compliance issues first';
            fb.style.color = 'var(--red)';
          }
        }
        return;
      }

      this.modalAuditState = 'SAVING';
      if (fb) fb.textContent = '⏳ Saving...';
      const currentCfg = { ...(this.cameras[name]?.config || { name: name, url: '' }) };
      currentCfg.normal_context = ctx;
      currentCfg.night_context_enabled = nightEnabled;
      currentCfg.night_context = ctxNight;
      const parseRuleText = (val) => {
        if (!val) return [];
        return val.split('\n').map(s => s.trim()).filter(s => s.length > 0);
      };
      currentCfg.severe_incidents = parseRuleText(severeText);
      currentCfg.low_incidents = parseRuleText(lowText);

      if (!this.cameras[name]) this.cameras[name] = {};
      this.cameras[name].config = currentCfg;
      await this._apiUpsertCamera(currentCfg);
      if (fb) {
        fb.textContent = '✅ Saved & Hot-Reloaded';
        fb.style.color = 'var(--green)';
        setTimeout(() => { fb.textContent = ''; }, 2500);
      }
      this.modalAuditState = 'SAVED';
      this._showToast(`Updated context & incident rules for ${name}`, 'ok');
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

    // Modal View Switcher handlers
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
    document.getElementById('btn-clear-alerts')?.addEventListener('click', async () => {
      this.alerts = [];
      this.alertCount = 0;
      this._renderAllAlerts();
      await this._authFetch('/api/alerts', { method: 'DELETE' }).catch(() => {});
      this._showToast('Cleared alerts feed', 'ok');
    });

    // Trigger Test Alert Dialog
    document.getElementById('btn-test-alert')?.addEventListener('click', () => this._openTestTriggerModal());
    document.getElementById('btn-close-test-trigger')?.addEventListener('click', () => this._closeTestTriggerModal());
    document.getElementById('btn-cancel-test-trigger')?.addEventListener('click', () => this._closeTestTriggerModal());
    document.getElementById('modal-test-trigger')?.addEventListener('click', e => {
      if (e.target.id === 'modal-test-trigger') this._closeTestTriggerModal();
    });
    document.getElementById('form-test-trigger')?.addEventListener('submit', (e) => { e.preventDefault(); this._submitTestTrigger(); });
    document.getElementById('btn-submit-test-trigger')?.addEventListener('click', (e) => { e.preventDefault(); this._submitTestTrigger(); });

    // VLM health check button
    document.getElementById('btn-check-vlm')?.addEventListener('click', () => this._fetchVLMEndpoints());

    // Settings Security & Lock events
    document.getElementById('btn-lock-settings')?.addEventListener('click', () => this._lockSettings());
    document.getElementById('btn-close-auth')?.addEventListener('click', () => this._closeAuthModal());
    document.getElementById('btn-cancel-auth')?.addEventListener('click', () => this._closeAuthModal());
    document.getElementById('modal-admin-auth')?.addEventListener('click', e => {
      if (e.target.id === 'modal-admin-auth') this._closeAuthModal();
    });
    document.getElementById('form-admin-auth')?.addEventListener('submit', (e) => { e.preventDefault(); this._submitAuth(); });
    document.getElementById('btn-submit-auth')?.addEventListener('click', (e) => { e.preventDefault(); this._submitAuth(); });
    document.getElementById('btn-toggle-pw-vis')?.addEventListener('click', () => {
      const pwInput = document.getElementById('auth-input-password');
      if (pwInput) {
        pwInput.type = pwInput.type === 'password' ? 'text' : 'password';
      }
    });
    document.getElementById('form-change-password')?.addEventListener('submit', (e) => { e.preventDefault(); this._submitPasswordChange(); });
    document.getElementById('btn-submit-pw-change')?.addEventListener('click', (e) => { e.preventDefault(); this._submitPasswordChange(); });

    // Reports events
    document.getElementById('btn-header-reports')?.addEventListener('click', () => this._openReportsModal());
    document.getElementById('btn-close-reports-modal')?.addEventListener('click', () => this._closeReportsModal());
    document.getElementById('reports-modal-backdrop')?.addEventListener('click', () => this._closeReportsModal());
    document.getElementById('btn-modal-generate-report')?.addEventListener('click', () => this._generateReportNow());
    document.getElementById('btn-refresh-reports-modal')?.addEventListener('click', () => {
      this._fetchReports();
      this._fetchReportingStats();
    });
    document.getElementById('btn-generate-report-now')?.addEventListener('click', () => this._generateReportNow());
    document.getElementById('btn-refresh-reports')?.addEventListener('click', () => {
      this._fetchReports();
      this._fetchReportingStats();
    });

    // Shift report filter chips
    ['all', 'DAY', 'NIGHT'].forEach(f => {
      const btn = document.querySelector(`[data-report-filter="${f}"]`);
      btn?.addEventListener('click', () => {
        document.querySelectorAll('[data-report-filter]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this._reportFilter = f;
        this._renderReportsCards();
      });
    });

    // Alert modal close
    document.getElementById('btn-close-alert-modal')?.addEventListener('click', () => this.closeAlertModal());
    document.getElementById('alert-modal-backdrop')?.addEventListener('click', () => this.closeAlertModal());

    // Archive Explorer Modal handlers
    document.getElementById('btn-open-archive')?.addEventListener('click', () => this._openArchiveModal());
    document.getElementById('btn-close-archive')?.addEventListener('click', () => this._closeArchiveModal());
    document.getElementById('archive-modal-backdrop')?.addEventListener('click', () => this._closeArchiveModal());
    document.getElementById('btn-refresh-archive')?.addEventListener('click', () => {
      this._fetchArchiveStats();
      this._fetchArchiveRecords();
    });
    document.getElementById('btn-archive-apply-filter')?.addEventListener('click', () => this._fetchArchiveRecords());
    document.getElementById('btn-archive-reset-filters')?.addEventListener('click', () => this._resetArchiveFilters());

    // Keyword search bindings
    const searchInput = document.getElementById('archive-filter-search');
    const searchClear = document.getElementById('archive-search-clear');
    let searchDebounce = null;

    searchInput?.addEventListener('input', (e) => {
      const val = e.target.value.trim();
      if (searchClear) searchClear.style.display = val ? 'inline-block' : 'none';
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => this._fetchArchiveRecords(), 350);
    });

    searchInput?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        clearTimeout(searchDebounce);
        this._fetchArchiveRecords();
      }
    });

    searchClear?.addEventListener('click', () => {
      if (searchInput) {
        searchInput.value = '';
        searchClear.style.display = 'none';
        this._fetchArchiveRecords();
      }
    });

    // Auto query on dropdown changes
    document.getElementById('archive-filter-cam')?.addEventListener('change', () => this._fetchArchiveRecords());
    document.getElementById('archive-filter-sev')?.addEventListener('change', () => this._fetchArchiveRecords());

    document.getElementById('btn-archive-range-1h')?.addEventListener('click', () => {
      this._setArchiveDatePreset('1h');
      this._fetchArchiveRecords();
    });
    document.getElementById('btn-archive-range-today')?.addEventListener('click', () => {
      this._setArchiveDatePreset('today');
      this._fetchArchiveRecords();
    });
    document.getElementById('btn-archive-range-24h')?.addEventListener('click', () => {
      this._setArchiveDatePreset('24h');
      this._fetchArchiveRecords();
    });
    document.getElementById('btn-archive-range-all')?.addEventListener('click', () => {
      this._setArchiveDatePreset('all');
      this._fetchArchiveRecords();
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') {
        this.closeAlertModal();
        this._closeModal();
        this._closeSettings();
        this._closeTestTriggerModal();
        this._closeArchiveModal();
        this._closeReportsModal();
        this._closeErrorsModal();
        this._closeVlmInspector();
        this._closeAuthModal();
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
      const res = await this._authFetch('/api/errors', { method: 'DELETE' });
      if (res.ok) {
        this._updateErrorBadge(0);
        this._showToast('Error log cleared', 'ok');
        this._fetchErrors();
      }
    } catch (e) {
      this._showToast(`Failed to clear errors: ${e.message}`, 'err');
    }
  },

  // ════════════════════════════════════════════════════════════
  //  Incident Archive & Timeline Explorer (3,000 Alert Sets)
  // ════════════════════════════════════════════════════════════
  // ════════════════════════════════════════════════════════════
  //  Incident Archive & Timeline Explorer (3,000 Alert Sets)
  // ════════════════════════════════════════════════════════════
  async _openArchiveModal() {
    const modal = document.getElementById('modal-archive');
    const backdrop = document.getElementById('archive-modal-backdrop');
    if (!modal) return;

    if (backdrop) backdrop.removeAttribute('hidden');
    modal.removeAttribute('hidden');

    // Populate camera dropdown
    const selCam = document.getElementById('archive-filter-cam');
    if (selCam) {
      const currentVal = selCam.value;
      const camNames = Object.keys(this.cameras || {});
      selCam.innerHTML = '<option value="">All Cameras</option>' + camNames.map(c => `<option value="${this._esc(c)}">${this._esc(c)}</option>`).join('');
      selCam.value = currentVal;
    }

    // Default to last 24 hours if empty
    const fromEl = document.getElementById('archive-filter-from');
    const toEl = document.getElementById('archive-filter-to');
    if (fromEl && !fromEl.value) {
      this._setArchiveDatePreset('24h');
    }

    await this._fetchArchiveStats();
    await this._fetchArchiveRecords();
  },

  _closeArchiveModal() {
    const modal = document.getElementById('modal-archive');
    const backdrop = document.getElementById('archive-modal-backdrop');
    if (modal) modal.setAttribute('hidden', '');
    if (backdrop) backdrop.setAttribute('hidden', '');
  },

  _resetArchiveFilters() {
    const searchInput = document.getElementById('archive-filter-search');
    const searchClear = document.getElementById('archive-search-clear');
    const selCam = document.getElementById('archive-filter-cam');
    const selSev = document.getElementById('archive-filter-sev');
    
    if (searchInput) searchInput.value = '';
    if (searchClear) searchClear.style.display = 'none';
    if (selCam) selCam.value = '';
    if (selSev) selSev.value = '';

    this._setArchiveDatePreset('24h');
    this._fetchArchiveRecords();
  },

  async _fetchArchiveStats() {
    const banner = document.getElementById('archive-stats-banner');
    try {
      const res = await fetch('/api/archive/stats');
      if (res.ok) {
        const stats = await res.json();
        const earliest = stats.earliest_ts ? new Date(stats.earliest_ts * 1000).toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'None';
        const latest = stats.latest_ts ? new Date(stats.latest_ts * 1000).toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'None';
        if (banner) {
          banner.textContent = `Rolling Buffer: ${stats.total_alert_sets || stats.total_analyses || 0} Alert Sets stored (${stats.total_frames || 0} frames) • Range: ${earliest} → ${latest}`;
        }
      }
    } catch (e) {
      if (banner) banner.textContent = 'Historical persistence active (analyses.db)';
    }
  },

  _setArchiveDatePreset(preset) {
    const now = new Date();
    const toEl = document.getElementById('archive-filter-to');
    const fromEl = document.getElementById('archive-filter-from');
    if (!toEl || !fromEl) return;

    // Highlight active preset pill
    const pills = document.querySelectorAll('.archive-preset-pills .preset-pill');
    pills.forEach(p => p.classList.remove('active'));
    const activePill = document.getElementById(`btn-archive-range-${preset}`);
    if (activePill) activePill.classList.add('active');

    const toStr = new Date(now.getTime() - now.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    toEl.value = toStr;

    let fromDate = new Date();
    if (preset === '1h') {
      fromDate = new Date(now.getTime() - 3600000);
    } else if (preset === 'today') {
      fromDate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
    } else if (preset === '24h') {
      fromDate = new Date(now.getTime() - 86400000);
    } else if (preset === 'all') {
      fromEl.value = '';
      toEl.value = '';
      return;
    }
    const fromStr = new Date(fromDate.getTime() - fromDate.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    fromEl.value = fromStr;
  },

  async _fetchArchiveRecords() {
    const searchVal = document.getElementById('archive-filter-search')?.value.trim() || '';
    const cam = document.getElementById('archive-filter-cam')?.value || '';
    const sev = document.getElementById('archive-filter-sev')?.value || '';
    const fromVal = document.getElementById('archive-filter-from')?.value;
    const toVal = document.getElementById('archive-filter-to')?.value;

    let sinceTs = null;
    let untilTs = null;
    if (fromVal) sinceTs = new Date(fromVal).getTime() / 1000;
    if (toVal) untilTs = new Date(toVal).getTime() / 1000;

    const params = new URLSearchParams({ limit: '150', offset: '0' });
    if (searchVal) params.set('search', searchVal);
    if (cam) params.set('cam', cam);
    if (sev) {
      if (sev === 'TRIGGER' || sev === 'FOLLOWUP') {
        params.set('trigger_mode', sev);
      } else {
        params.set('severity', sev);
      }
    }
    if (sinceTs) params.set('since', sinceTs.toString());
    if (untilTs) params.set('until', untilTs.toString());

    try {
      const res = await fetch(`/api/history?${params.toString()}`);
      if (res.ok) {
        const records = await res.json();
        this._renderArchiveRecords(records);
      }
    } catch (e) {
      console.warn('Failed to query archive records:', e);
    }
  },

  _renderArchiveRecords(records = []) {
    const tbody = document.getElementById('archive-records-body');
    const countEl = document.getElementById('archive-matched-count');
    const emptyEl = document.getElementById('archive-empty');
    if (!tbody) return;

    if (countEl) countEl.textContent = records.length;
    tbody.innerHTML = '';

    if (!records || records.length === 0) {
      if (emptyEl) emptyEl.style.display = 'flex';
      return;
    }
    if (emptyEl) emptyEl.style.display = 'none';

    records.forEach((rec, idx) => {
      const tr = document.createElement('tr');
      tr.className = 'archive-record-row';
      const timeStr = rec.ts ? new Date(rec.ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—';
      const sev = (rec.severity || 'LOW').toUpperCase();
      const sevColor = sev === 'HIGH' ? '#ef4444' : (sev === 'MEDIUM' ? '#f59e0b' : '#10b981');
      const obs = (rec.observation || 'Analysis recorded');

      tr.innerHTML = `
        <td style="white-space: nowrap; font-family: var(--font-mono); font-size: 0.72rem; color: var(--text-tertiary);">${this._esc(timeStr)}</td>
        <td style="font-weight: 600; color: var(--text-primary);">${this._esc(rec.cam || '')}</td>
        <td><span class="badge" style="background: ${sevColor}20; color: ${sevColor}; font-size: 0.65rem; font-weight: 700;">${sev}</span></td>
        <td style="color: var(--text-secondary); max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${this._esc(obs)}">${this._esc(obs)}</td>
      `;

      tr.addEventListener('click', () => {
        tbody.querySelectorAll('.archive-record-row').forEach(r => r.classList.remove('selected'));
        tr.classList.add('selected');
        this._selectArchiveRecord(rec);
      });

      tbody.appendChild(tr);

      // Auto-select first item
      if (idx === 0) {
        tr.classList.add('selected');
        this._selectArchiveRecord(rec);
      }
    });
  },

  async _selectArchiveRecord(rec) {
    const titleEl = document.getElementById('archive-ins-title');
    const idEl = document.getElementById('archive-ins-id');
    const obsEl = document.getElementById('archive-ins-obs');
    const badgeEl = document.getElementById('archive-ins-badge');
    const stripEl = document.getElementById('archive-frames-strip');
    const mainImg = document.getElementById('archive-main-preview');
    const phEl = document.getElementById('archive-preview-placeholder');
    const tagEl = document.getElementById('archive-frame-tag');

    const incId = rec.incident_id || rec.id || `INC-${rec.cam}-${rec.id}`;
    if (titleEl) titleEl.textContent = `${rec.cam} Alert Set Sequence`;
    if (idEl) idEl.textContent = `${incId} • ${rec.ts ? new Date(rec.ts * 1000).toLocaleString() : ''}`;
    if (obsEl) obsEl.textContent = rec.observation || 'No visual anomalies reported.';

    const sev = (rec.severity || 'LOW').toUpperCase();
    if (badgeEl) {
      badgeEl.textContent = `${sev} SEVERITY`;
      badgeEl.className = `badge ${sev === 'HIGH' ? 'badge-red' : (sev === 'MEDIUM' ? 'badge-amber' : 'badge-green')}`;
      badgeEl.style.display = 'inline-block';
    }

    if (!stripEl) return;
    stripEl.innerHTML = `
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">Loading...</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">Loading...</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">Loading...</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">Loading...</div></div>
    `;

    try {
      const res = await fetch(`/api/incidents/${encodeURIComponent(incId)}/frames`);
      if (res.ok) {
        const data = await res.json();
        const frames = data.frames || [];
        if (frames.length > 0) {
          stripEl.innerHTML = frames.map((f, idx) => `
            <div class="archive-frame-thumb-box ${idx === frames.length - 1 ? 'active' : ''}" data-idx="${idx}" title="Frame #${idx + 1} (t-${frames.length - 1 - idx})">
              <img src="data:image/jpeg;base64,${f.b64}" alt="Frame ${idx + 1}" />
              <span class="cam-event-thumb-badge" style="position: absolute; bottom: 3px; right: 3px; background: rgba(0,0,0,0.7); font-size: 0.65rem; padding: 1px 4px; border-radius: 3px; font-family: var(--font-mono); color: #fff;">t-${frames.length - 1 - idx}</span>
            </div>
          `).join('');

          // Bind clicks on frame thumbnails
          const boxes = stripEl.querySelectorAll('.archive-frame-thumb-box');
          boxes.forEach((box, idx) => {
            box.addEventListener('click', () => {
              boxes.forEach(b => b.classList.remove('active'));
              box.classList.add('active');
              if (mainImg) {
                mainImg.src = `data:image/jpeg;base64,${frames[idx].b64}`;
                mainImg.style.display = 'block';
              }
              if (phEl) phEl.style.display = 'none';
              if (tagEl) {
                tagEl.textContent = `Frame #${idx + 1} (t-${frames.length - 1 - idx})`;
                tagEl.style.display = 'inline-block';
              }
            });
          });

          // Show trigger frame (last frame) by default
          const lastIdx = frames.length - 1;
          if (mainImg) {
            mainImg.src = `data:image/jpeg;base64,${frames[lastIdx].b64}`;
            mainImg.style.display = 'block';
          }
          if (phEl) phEl.style.display = 'none';
          if (tagEl) {
            tagEl.textContent = `Trigger Frame #${lastIdx + 1} (t-0)`;
            tagEl.style.display = 'inline-block';
          }
          return;
        }
      }
    } catch (e) {
      console.warn('Could not load 4-frame set from DB:', e);
    }

    // Fallback if no frames were saved for this row
    stripEl.innerHTML = `
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">t-3</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">t-2</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">t-1</div></div>
      <div class="archive-frame-thumb-box"><div class="archive-frame-ph">t-0</div></div>
    `;
    if (mainImg) mainImg.style.display = 'none';
    if (phEl) {
      phEl.textContent = '📸 No stored frame sequence for this earlier record.';
      phEl.style.display = 'block';
    }
    if (tagEl) tagEl.style.display = 'none';
  },
};

// Global helper exports for inline handlers / external invocation (multicam_behavior_test style)
window.changePage = (delta) => App.changePage(delta);
window.setCamPage = (page) => App.setCamPage(page);
window.setLayout = (cams) => App.setLayout(cams);
window.jumpToCamera = (cam) => App.jumpToCamera(cam);

document.addEventListener('DOMContentLoaded', () => App.init());
