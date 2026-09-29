# ⚡ RapidAlert: Hybrid VLM Edge Surveillance Engine
### Autonomous Multi-Camera Reasoning & Dual-Tier Scene Shift Intelligence on NVIDIA Jetson AGX Thor

[![Platform](https://img.shields.io/badge/Platform-NVIDIA%20Jetson%20AGX%20Thor-76B900?logo=nvidia&logoColor=white)](https://www.nvidia.com)
[![VLM](https://img.shields.io/badge/VLM-Cosmos%20Reason2%208B%20(NVFP4)-blueviolet)](https://huggingface.co/vrfai/Cosmos-Reason2-8B-NVFP4)
[![Trigger](https://img.shields.io/badge/Trigger-DINOv2%20Embedding%20Drift%20(10ms)-06B6D4)](#-dual-tier-visual-intelligence-pipeline)
[![Streaming](https://img.shields.io/badge/Streaming-Direct%20Hardware%20HLS%20%2F%20WebRTC-FF4081)](#-direct-hardware-rtsp-streaming--low-latency-hls)
[![MIG](https://img.shields.io/badge/MIG-12%20SM%20%2B%208%20SM%20Sharding-orange)](#-nvidia-thor-mig-partitioning-deepdive)
[![Queuing](https://img.shields.io/badge/Queue-Two--Tier%20Async%20Priority%20%2B%20Weighted%20MIG-critical)](#-two-tier-in-memory-queuing--load-balancing-system)
[![Watchdog](https://img.shields.io/badge/Watchdog-Real--Time%20Alerts%20%26%2024h%20Digest-red)](#-automated-health-watchdog--real-time-alerting)
[![Reporting](https://img.shields.io/badge/Reporting-Shift%20PDF%20%26%2069--Report%20Buffer-success)](#-automated-shift-reporting--pdf-generation)
[![Inference Engine](https://img.shields.io/badge/Engine-vLLM%200.19.0%20(PagedAttention)-green)](https://github.com/vllm-project/vllm)

RapidAlert is an enterprise-grade, edge-native surveillance and reasoning platform engineered specifically for **NVIDIA Jetson AGX Thor**. It bridges real-time GStreamer/NVDEC hardware camera ingestion, direct low-latency hardware streaming (MediaMTX / Hls.js), and microsecond visual embedding drift detection with deep multimodal reasoning from **Cosmos Reason2 8B (NVFP4)** across partitioned **Multi-Instance GPU (MIG)** compute shards.

---

## 📑 Table of Contents

1. [Architectural Overview](#-architectural-overview)
2. [Dual-Tier Visual Intelligence Pipeline](#-dual-tier-visual-intelligence-pipeline)
   - [Tier 1: Real-Time Scene Trigger (DINOv2)](#tier-1-real-time-scene-trigger-dinov2)
   - [Tier 2: Temporal Multi-Frame Reasoning (Cosmos Reason2 8B)](#tier-2-temporal-multi-frame-reasoning-cosmos-reason2-8b)
3. [Direct Hardware RTSP Streaming & Low-Latency HLS](#-direct-hardware-rtsp-streaming--low-latency-hls)
   - [Embedded MediaMTX Gateway & Hardware GPU Passthrough](#embedded-mediamtx-gateway--hardware-gpu-passthrough)
   - [Zero-Transcoding HTML5 Video Decoding (Hls.js)](#zero-transcoding-html5-video-decoding-hlsjs)
   - [Automatic Dual-Mode Fallback (MJPEG Condition Streaming)](#automatic-dual-mode-fallback-mjpeg-condition-streaming)
4. [Active-Page Multi-Stream Viewport & Dynamic Grid Pagination](#-active-page-multi-stream-viewport--dynamic-grid-pagination)
   - [Dynamic 4-Stream (2×2) vs 6-Stream (3×2) Grid Layouts](#dynamic-4-stream-22-vs-6-stream-32-grid-layouts)
   - [Strict Active-Page Ingestion Policy (Zero Inactive Overhead)](#strict-active-page-ingestion-policy-zero-inactive-overhead)
   - [Thread-Safe Monotonic Sequence Tracking (`cur_seq`)](#thread-safe-monotonic-sequence-tracking-cur_seq)
   - [Dynamic Stream Directory & Active Socket Registry](#dynamic-stream-directory--active-socket-registry)
5. [Two-Tier In-Memory Queuing & Load Balancing System](#-two-tier-in-memory-queuing--load-balancing-system)
   - [Tier A: Global Dispatch Priority Queue (`PriorityQueue`)](#tier-a-global-dispatch-priority-queue-priorityqueue)
   - [Tier B: Per-MIG Endpoint Worker Shard Queues (`_EndpointShard`)](#tier-b-per-mig-endpoint-worker-shard-queues-_endpointshard)
   - [Deadlock Prevention & In-Flight Lock Deduplication](#deadlock-prevention--in-flight-lock-deduplication)
   - [Cold-Boot Model Warmup & Circuit Breaking](#cold-boot-model-warmup--circuit-breaking)
6. [NVIDIA Thor Hardware & Unified Memory Architecture](#-nvidia-thor-hardware--unified-memory-architecture)
   - [Unified Memory Dynamics](#unified-memory-dynamics)
   - [RAM Budgeting & KV Cache Sizing](#ram-budgeting--kv-cache-sizing)
   - [Dynamic GPU Activity Calculation (NVML Power Curve)](#dynamic-gpu-activity-calculation-nvml-power-curve)
7. [NVIDIA Thor MIG Partitioning Deepdive](#-nvidia-thor-mig-partitioning-deepdive)
   - [Hardware Slices (Profile 83 + Profile 78)](#hardware-slices-profile-83--profile-78)
   - [Container Device Interface (CDI) & Device Capabilities](#container-device-interface-cdi--device-capabilities)
8. [Autonomous Scheduler & Follow-Up Lifecycle](#-autonomous-scheduler--follow-up-lifecycle)
9. [Automated Health Watchdog & Real-Time Alerting](#-automated-health-watchdog--real-time-alerting)
   - [Real-Time Critical Incident Alerting Criteria](#real-time-critical-incident-alerting-criteria)
   - [Hardware Sentinel & Power Outage / Crash Recovery](#hardware-sentinel--power-outage--crash-recovery)
   - [System Restarts & Thor Hardware Reboot Notifications](#system-restarts--thor-hardware-reboot-notifications)
   - [24-Hour Automated System Health & Diagnostics Digest](#24-hour-automated-system-health--diagnostics-digest)
10. [Automated Shift Reporting & PDF Generation](#-automated-shift-reporting--pdf-generation)
    - [Scheduled Shift Reports (06:00 AM & 06:00 PM IST)](#scheduled-shift-reports-0600-am--0600-pm-ist)
    - [Executive PDF Report Architecture (ReportLab)](#executive-pdf-report-architecture-reportlab)
    - [69-Report Rolling Circular Buffer Storage (~1 Month Archive)](#69-report-rolling-circular-buffer-storage-1-month-archive)
    - [Frontend Shift Intelligence Archive Modal](#frontend-shift-intelligence-archive-modal)
11. [Fault Tolerance & Structured Error Tracking](#-fault-tolerance--structured-error-tracking)
12. [Configuration Reference](#-configuration-reference)
13. [REST API Specification](#-rest-api-specification)
14. [Deployment & Operations Guide](#-deployment--operations-guide)

---

## 🏗️ Architectural Overview

```mermaid
flowchart TB
    subgraph CAMERAS["0. CCTV Subnet"]
        RTSP["7× Dahua/Hikvision IP Cameras<br>(1080p / 4K @ 25-30 FPS)"]
    end

    subgraph DIRECT_STREAM["1. Direct Hardware Streaming Layer (MediaMTX)"]
        RTSP -->|Raw TCP RTSP Passthrough| MMTX["Embedded MediaMTX Gateway (Port 8888 / 8889)"]
        MMTX -->|Low-Latency HLS (200ms parts)| HLS_FEED["Hls.js HTML5 Video Engine (Port 8888)"]
        MMTX -->|WebRTC / WHEP Passthrough| WHEP_FEED["WebRTC Client Player (Port 8889)"]
    end

    subgraph INGEST["2. Ingestion & Feature Extraction Layer (NVDEC & DINOv2)"]
        RTSP --> NVDEC["NVDEC Hardware Decoder (nvurisrcbin)"]
        NVDEC -->|Fallback on Bufferpool Error| CPU_FALLBACK["CPU OpenCV Fallback Engine"]
        NVDEC --> CONV["nvvideoconvert (NVMM -> BGR/RGB)"]
        CPU_FALLBACK --> STORE["Thread-Safe FrameStore (Rolling Ring Buffer + Monotonic Seq)"]
        CONV --> STORE
        STORE --> DINO["DINOv2 Feature Extractor (facebook/dinov2-small)"]
        DINO --> COSINE["Cosine Distance Drift Engine"]
        COSINE --> CLASSIFY{"Drift vs Thresholds"}
        CLASSIFY -->|">= Major (0.033)"| TIER2["🚨 Tier-2 Major Incident (Priority 0)"]
        CLASSIFY -->|">= Minor (0.016)"| TIER3["⚠️ Tier-3 Minor Shift (Priority 1)"]
        CLASSIFY -->|"< Minor"| HB["💓 Tier-4 Heartbeat (Priority 3, 35s Cadence)"]
    end

    subgraph QUEUE_LAYER["3. Two-Tier In-Memory Async Queuing System"]
        TIER2 --> PRIO_Q["Tier A: Global Dispatch Priority Queue<br>(In-Memory asyncio.PriorityQueue, Zero-IPC)"]
        TIER3 --> PRIO_Q
        HB --> PRIO_Q
        PRIO_Q --> ROUTER["Weighted Least-Connections Load Balancer<br>load_score = (in_flight + queued) / weight"]
        ROUTER --> SHARD_Q0["Tier B1: Shard 0 Bounded Queue<br>(asyncio.Queue, max=12, sem=4)"]
        ROUTER --> SHARD_Q1["Tier B2: Shard 1 Bounded Queue<br>(asyncio.Queue, max=12, sem=4)"]
    end

    subgraph MIG["4. Hardware-Isolated MIG Inference (Tier 2)"]
        SHARD_Q0 --> VLLM0["vLLM Shard 0 (Port 8000)<br>MIG Profile 83 (12 SMs + 3D GFX)<br>weight=3, gpu_util=0.33 (~40.5 GiB)"]
        SHARD_Q1 --> VLLM1["vLLM Shard 1 (Port 8001)<br>MIG Profile 78 (8 SMs + Media)<br>weight=2, gpu_util=0.30 (~36.8 GiB)"]
        VLLM0 --> VLM["Cosmos Reason2 8B NVFP4<br>(4 Temporal Interleaved Frames)"]
        VLLM1 --> VLM
    end

    subgraph PERSIST["5. Follow-Up, Storage & Alerting"]
        VLM --> ALERT_ENG["Alert Engine & Rules Classifier"]
        ALERT_ENG -->|Severity == HIGH/DANGER| FOLLOW["Follow-Up Scheduler (10s Cycle, Max 6)"]
        FOLLOW -->|Priority 0| PRIO_Q
        ALERT_ENG --> SQLITE[("SQLite Database (WAL Mode)<br>data/analyses.db")]
    end

    subgraph SERVICES["6. Watchdog, Reporting & Email Dispatcher"]
        WATCHDOG["SystemWatchdog (15s Loop)"] --> SENTINEL["Heartbeat Sentinel (data/system_state.json)"]
        WATCHDOG --> WATCH_EMAIL["WatchdogEmailer (Zero-Lag Async Background Dispatch)"]
        REPORTS["ReportingService (Shift Scheduler: 06:00 & 18:00 IST)"] --> PDF_GEN["ReportLab Multi-Page PDF Engine"]
        PDF_GEN --> REPORT_STORE["69-Report Rolling Buffer (data/reports/)"]
        WATCH_EMAIL --> SMTP["Gmail SMTP (reportsclove@gmail.com)"]
        PDF_GEN --> SMTP
        SMTP --> RECIPIENTS["Recipients: pandalavacarji@gmail.com, reportsclove@gmail.com"]
    end

    subgraph UI["7. Frontend UI & Active-Page Viewport"]
        HLS_FEED --> DASH["Live Monitoring Dashboard (4-Stream / 6-Stream Paginated Grid)"]
        STORE -.->|Fallback Stream /api/cameras/{name}/stream| DASH
        ALERT_ENG -.->|Real-Time Incidents| WS["WebSocket Manager (/ws)"]
        SQLITE -.-> API["FastAPI REST Backend (Port 7000)"]
        WS --> DASH
        API --> DASH
    end
```

---

## 🧠 Dual-Tier Visual Intelligence Pipeline

### Tier 1: Real-Time Scene Trigger (DINOv2)
* **Model**: `facebook/dinov2-small` (384-dimensional dense visual representations).
* **Execution Latency**: **~8–12 ms** per frame on CPU / CUDA.
* **Mechanism**:
  1. Ingests full-frame camera snapshots directly from physical memory.
  2. Compares the current spatial embedding $E_t$ against moving baseline embedding $E_{\text{baseline}}$ using normalized Cosine Distance:
     $$\text{Drift}(t) = 1 - \frac{E_t \cdot E_{\text{baseline}}}{\|E_t\|_2 \|E_{\text{baseline}}\|_2}$$
  3. **Multi-Threshold Decision Tree**:
     * **Major Incident (Priority 0)**: $\text{Drift} \ge \text{dino\_major\_threshold}$ (Default: `0.033`). Dispatches emergency multimodal reasoning immediately.
     * **Minor Scene Shift (Priority 1)**: $\text{Drift} \ge \text{dino\_minor\_threshold}$ (Default: `0.0165`). Captures perimeter breaches, unauthorized movement, or physical layout alterations.
     * **Periodic Heartbeat (Priority 3)**: Routine ambient inspection every 35s.

### Tier 2: Temporal Multi-Frame Reasoning (Cosmos Reason2 8B)
* **Model**: `vrfai/Cosmos-Reason2-8B-NVFP4` running on **vLLM 0.19.0** with PagedAttention.
* **Hardware Weights**: NVIDIA NVFP4 quantization (4-bit floating point weights with FP8 activations).
* **Temporal Attention Sequence**:
  Captures **4 ordered temporal snapshots** ($t-10s, t-6.5s, t-3s, t-0s$) interleaved with explicit contextual vision anchors:
  ```json
  [
    {"type": "text", "text": "Frame 1 (t -10.0s):"},
    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}},
    {"type": "text", "text": "Frame 2 (t -6.5s):"},
    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}},
    {"type": "text", "text": "Frame 3 (t -3.0s):"},
    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}},
    {"type": "text", "text": "Frame 4 (t 0.0s - CURRENT TRIGGER):"},
    {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,..."}},
    {"type": "text", "text": "Analyze the temporal sequence across these 4 surveillance frames. Identify what initiated the scene shift, who is involved, and classify safety hazards."}
  ]
  ```

> [!NOTE]
> **VLM Alignment & "Lawyer Mode" Workarounds**: Qwen-based models are aggressively alignment-tuned via RLHF to act as helpful, harmless assistants, which can interfere with strict threat assessment. For a deep dive into how RapidAlert bypasses this safety alignment using System Prompts and literal rule definitions, see [VLM Alignment Workarounds](docs/vlm_alignment_workarounds.md).

---

## 📺 Direct Hardware RTSP Streaming & Low-Latency HLS

Web browsers cannot natively decode raw RTSP (`rtsp://`) streams over UDP/TCP without a gateway. RapidAlert integrates an **Embedded MediaMTX Hardware Streaming Gateway** to eliminate server CPU encoding bottlenecks.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   DIRECT HARDWARE RTSP STREAMING TOPOLOGY                   │
├──────────────────────────┬──────────────────────────┬───────────────────────┤
│    Camera Ingestion      │   Media Gateway Route    │  Browser Presentation │
├──────────────────────────┼──────────────────────────┼───────────────────────┤
│ 7× IP Cameras            │ Embedded MediaMTX        │ HTML5 <video> with    │
│ H.264/H.265 over TCP     │ Daemon (Port 8888/8889)  │ Hls.js hardware decode│
│ 1080p / 4K @ 25-30 FPS   │ Zero transcoding         │ 0% CPU, sub-sec delay │
└──────────────────────────┴──────────────────────────┴───────────────────────┘
```

### Embedded MediaMTX Gateway & Hardware GPU Passthrough
* **Process Management**: Started and supervised by `backend/services/mediamtx_service.py` during backend startup.
* **Protocol Passthrough**: Proxies raw RTSP packets directly into Low-Latency HLS (LL-HLS) on port `8888` and WebRTC/WHEP on port `8889`.
* **Zero Transcoding**: Packets pass directly from camera network sockets to HTTP endpoints without CPU decompression or re-compression.

### Zero-Transcoding HTML5 Video Decoding (Hls.js)
* In the frontend (`frontend/app.js`), each camera card mounts an HTML5 `<video>` element attached to `Hls.js`.
* **Client-Side Hardware Acceleration**: Chromium utilizes the Jetson AGX Thor GPU hardware video decoder (VDPAU / NVDEC) directly inside the browser process.
* **Performance**: Yields fluid **30–60 FPS** motion with sub-second latency while consuming **0% Python server CPU**.

### Automatic Dual-Mode Fallback (MJPEG Condition Streaming)
* If Hls.js encounters an unsupported browser environment or an initializing stream, it automatically falls back to RapidAlert's direct `/api/cameras/{name}/stream` endpoint.
* In the backend, `FrameStore` utilizes a thread-safe `threading.Condition` and monotonically increasing sequence numbers (`cur_seq`) so that MJPEG clients receive immediate frame delivery with zero polling jitter.

---

## 🎛️ Active-Page Multi-Stream Viewport & Dynamic Grid Pagination

Streaming 7 concurrent 1080p camera feeds simultaneously to a single browser window can overwhelm browser memory and socket pools. RapidAlert implements an **Active-Page Viewport Policy**.

### Dynamic 4-Stream (2×2) vs 6-Stream (3×2) Grid Layouts
* **2×2 Mode (Default)**: Displays 4 high-resolution cameras per page (Page 1: 4 cams, Page 2: 3 cams).
* **3×2 Mode**: Displays 6 cameras per page (Page 1: 6 cams, Page 2: 1 cam).
* Responsive layout toggle controls dynamically adapt grid columns and aspect ratios.

### Strict Active-Page Ingestion Policy (Zero Inactive Overhead)
* **Automatic Socket Detachment**: When navigating between Page 1 and Page 2, all cameras not on the active page have their streams immediately torn down (`_detachDirectStream(name)`).
* **Zero Backend Compression**: `FrameStore.get_frame_since()` executes encoding exclusively for feeds with active consumers. Inactive feeds incur **zero JPEG encoding overhead**.
* **Instant Reconnection**: Navigating to a page attaches live hardware streams with fresh cache-busting connection timestamps (`&t=${Date.now()}`), preventing stale browser socket locks.

### Dynamic Stream Directory & Active Socket Registry
* `backend/main.py` maintains an active stream registry via `_register_stream_start(name)` and `_register_stream_stop(name)`.
* Real-time active stream counts and camera assignments are exposed via `GET /api/cameras/active-streams` and broadcasted over WebSockets to all connected dashboard operators.

---

## 🔀 Two-Tier In-Memory Queuing & Load Balancing System

Surveillance inference requests can spike instantaneously when multiple cameras detect movement at the same second. RapidAlert deploys a **Two-Tier Async In-Memory Scheduling Architecture** (`backend/services/scheduler.py` & `backend/services/vlm_pool.py`).

```
Trigger Event (Tier 2 / Tier 3 / Follow-Up)
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│     Tier A: Global Dispatch Priority Queue              │
│     (In-Memory asyncio.PriorityQueue, Zero IPC)         │
│     • Priority 0: Critical Follow-Up & Major Incident   │
│     • Priority 1: Minor Scene Shift                     │
│     • Priority 3: Routine Ambient Heartbeat             │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼  Weighted Least-Connections Router
┌─────────────────────────────────────────────────────────┐
│     Tier B: Per-MIG Endpoint Worker Shard Queues        │
├────────────────────────────┬────────────────────────────┤
│   Shard 0 (MIG 12 SMs)     │   Shard 1 (MIG 8 SMs)      │
│   • Weight: 3              │   • Weight: 2              │
│   • Bounded Queue: max=12  │   • Bounded Queue: max=12  │
│   • Concurrency: sem=4     │   • Concurrency: sem=4     │
└────────────────────────────┴────────────────────────────┘
```

### Deadlock Prevention & In-Flight Lock Deduplication
* **Per-Camera In-Flight Locks**: `_camera_locks[cam]` prevents duplicate redundant inferences when a camera produces rapid micro-drifts while an inference is already actively processing.
* **Bounded Shard Queues**: If all vLLM shards reach saturation, excess requests wait safely in Tier A rather than overloading vLLM HTTP workers.

---

## 🛡️ Automated Health Watchdog & Real-Time Alerting

RapidAlert runs an autonomous health monitor (`backend/services/watchdog.py`) alongside an asynchronous email dispatch engine (`backend/services/watchdog_emailer.py`).

### Real-Time Critical Incident Alerting Criteria
Immediate critical incident emails are dispatched via Gmail SMTP (`reportsclove@gmail.com`) upon detection of:
1. **`SYSTEM_RESTART` / Crash Recovery**: System reboots or unexpected crash recoveries detected on startup.
2. **`CAMERA_BLACKOUT`**: More than 50% or all camera feeds disconnected/frozen for $>30\text{ seconds}$.
3. **`VLLM_CLUSTER_DOWN`**: All partitioned MIG inference shards offline or failing HTTP health checks.
4. **`STORAGE_CRITICAL`**: Disk free space falls below $5\%$ or SQLite database corruption occurs.
5. **`FATAL_EXCEPTION`**: Unhandled critical exceptions in core video ingestion or analysis loops.

### Anti-Spam Throttling & Resolution Notices
* **Cooldown Deduplication**: A 10-minute cooldown window per active fault category prevents duplicate alert storms during sustained outages.
* **`[RESOLVED]` Notifications**: When an active fault condition clears (e.g. cameras reconnect or vLLM shards recover), an automatic green resolution email is dispatched to administrators.

### 24-Hour Automated System Health & Diagnostics Digest
* **Schedule**: Automatically compiled and dispatched daily at **08:00 AM IST**.
* **KPI Telemetry**:
  * Total 24h operational uptime and restart counts.
  * Total multimodal VLM analyses completed.
  * Structured error breakdown categorized by severity (`FATAL`, `CRITICAL`, `WARNING`).
  * Camera reliability index (NVDEC Hardware vs CPU Fallback ratios).
  * vLLM dual-shard inference health and response latencies.

---

## 📊 Automated Shift Reporting & PDF Generation

RapidAlert generates executive shift intelligence reports (`backend/services/reporting_service.py`) summarizing all surveillance activity across daily operational shifts.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SHIFT REPORT GENERATION & BUFFERING                      │
├──────────────────────────┬──────────────────────────┬───────────────────────┤
│    Scheduled Triggers    │  Executive PDF Engine    │ Circular Report Buffer│
├──────────────────────────┼──────────────────────────┼───────────────────────┤
│ • 06:00 AM IST (Night)   │ ReportLab Multi-Page PDF │ 69 Historical Reports │
│ • 06:00 PM IST (Day)     │ Embedded Event Frames    │ (~1 Month of Shifts)  │
│ • On-Demand Admin REST   │ DINOv2 Drift & Safety    │ data/reports/         │
└──────────────────────────┴──────────────────────────┴───────────────────────┘
```

### 69-Report Rolling Circular Buffer Storage (~1 Month Archive)
* The report manager maintains a circular rolling buffer of **69 past shift reports** in `data/reports/`.
* When report #70 is generated, the oldest report is automatically pruned, ensuring predictable disk utilization (~50–100 MB total storage).
* Accessible on the dashboard via the **Reports Archive Modal** (located on the top navigation bar near the MIG indicator).

---

## ⚙️ Configuration Reference

### `config/system.json`
```json
{
  "dino_major_threshold": 0.033,
  "dino_minor_threshold": 0.0165,
  "followup_interval_sec": 10.0,
  "followup_max_cycles": 6,
  "reporting_enabled": true,
  "report_schedule_hours": [6, 18],
  "report_sender_email": "reportsclove@gmail.com",
  "report_sender_password": "wakl rpps mvql eznn",
  "report_email_recipients": [
    "pandalavacarji@gmail.com",
    "reportsclove@gmail.com"
  ],
  "watchdog_alerts_enabled": true,
  "watchdog_email_recipients": [
    "pandalavacarji@gmail.com",
    "reportsclove@gmail.com"
  ],
  "daily_digest_enabled": true,
  "daily_digest_hour": 8
}
```

---

## 📡 REST API Specification

| Method | Endpoint | Description | Auth Required |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/status` | System operational summary, feed health, and vLLM status | No |
| `GET` | `/api/cameras` | Active camera list and ingestion backend status | No |
| `GET` | `/api/cameras/active-streams` | Real-time directory of actively pulled streams and consumers | No |
| `GET` | `/api/cameras/{name}/stream-urls` | Direct WebRTC, WHEP, and Low-Latency HLS stream endpoints | No |
| `GET` | `/api/cameras/{name}/stream` | Direct optimized MJPEG stream with sequence tracking | No |
| `GET` | `/api/drifts` | Real-time DINOv2 visual drift scores across all feeds | No |
| `GET` | `/api/alerts` | Historical incident feed and security alerts | No |
| `GET` | `/api/errors` | Structured error logs and stack traces | No |
| `GET` | `/api/errors/summary` | Error aggregation by component, severity, and effect | No |
| `GET` | `/api/metrics` | Hardware telemetry (GPU power, memory, temperatures) | No |
| `GET` | `/api/reports` | List of stored shift intelligence PDF reports (69-buffer) | No |
| `GET` | `/api/reports/{id}/pdf`| Download specific shift report PDF | No |
| `POST` | `/api/reports/generate`| Generate custom shift report on-demand | No |
| `GET` | `/api/reports/stats` | Reporting schedule, buffer status, and next run time | No |
| `GET` | `/api/watchdog/status` | Health watchdog telemetry and 24h diagnostic snapshot | No |
| `POST` | `/api/watchdog/test-alert` | Dispatch a test critical incident alert email | Yes (Admin) |
| `POST` | `/api/watchdog/send-daily-digest` | Force compile & dispatch 24h health digest | Yes (Admin) |
| `POST` | `/api/cameras` | Add or update IP camera configuration | Yes (Admin) |
| `DELETE` | `/api/cameras/{name}` | Remove camera stream | Yes (Admin) |
| `POST` | `/api/auth/login` | Authenticate admin session and issue bearer token | No |
| `WS` | `/ws` | Real-time binary/JSON telemetry, alerts, and active stream state | No |

---

## 🚀 Deployment & Operations Guide

### Quick Start
```bash
# 1. Start RapidAlert
./run.sh
```
* Runs the automated pre-flight integrity audit.
* Validates dual MIG vLLM containers on ports `8000` and `8001`.
* Launches the MediaMTX direct hardware gateway and FastAPI backend.
* Opens the live dashboard on **`http://localhost:7000`**.

### Clean Shutdown
```bash
# 2. Graceful Shutdown
./stop.sh
```
* Safely terminates all NVDEC decoders and MediaMTX daemons.
* Records clean sentinel state in `data/system_state.json`.
* Flushes WAL SQLite transactions cleanly.
