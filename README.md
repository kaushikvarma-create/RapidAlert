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
   - [Supported MIG Profiles on NVIDIA Thor (Official JetPack 7.2 Matrix)](#supported-mig-profiles-on-nvidia-thor-official-jetpack-72-matrix)
   - [Optimal Dual-MIG Production Pairing (Profile 83 + Profile 78)](#optimal-dual-mig-production-pairing-profile-83--profile-78)
   - [Automated Partitioning Script (`scripts/create-thor-mig.sh`)](#automated-partitioning-script-scriptscreate-thor-migsh)
   - [Container Device Interface (CDI) & Device Capabilities (`/dev/nvidia-caps`)](#container-device-interface-cdi--device-capabilities-devnvidia-caps)
8. [Autonomous Scheduler & Follow-Up Lifecycle](#-autonomous-scheduler--follow-up-lifecycle)
9. [Automated Health Watchdog & Real-Time Alerting](#-automated-health-watchdog--real-time-alerting)
   - [Automated Container Crash Watchdog & Self-Healing Engine](#automated-container-crash-watchdog--self-healing-engine)
   - [Crash-Loop Backoff & Anti-Thrashing Guardrails](#crash-loop-backoff--anti-thrashing-guardrails)
   - [Operator Diagnostic Solving Tools & Incident Emails](#operator-diagnostic-solving-tools--incident-emails)
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

## ⚡ NVIDIA Thor Hardware & Unified Memory Architecture

### Unified Memory Dynamics
On NVIDIA Jetson AGX Thor, CPU host memory and GPU video memory share a single contiguous physical address space (**128 GB 256-bit LPDDR5X Unified Memory** delivering >200 GB/s bandwidth).
* **Zero-Copy Ingestion**: Frame buffers decoded by hardware NVDEC are mapped directly into CUDA tensors and DINOv2 feature vectors without CPU-to-GPU PCIe memory copying.
* **Shared Silicon Ceiling**: Every vLLM PagedAttention KV-cache block, OS buffer, and desktop frame buffer directly draws from physical LPDDR5X system RAM.

### RAM Budgeting & KV Cache Sizing
To prevent Out-Of-Memory (OOM) kernel panics on Jetson Thor's 122.8 GiB usable space, memory budgets are strictly compartmentalized:
```
Total Usable LPDDR5X Memory: 122.8 GiB (128 GB Physical)
├── vLLM Instance 0 (Port 8000, 12 SMs) ── 0.33 util ── 40.5 GiB (7.35 GB NVFP4 Weights + 33.15 GB KV Cache)
├── vLLM Instance 1 (Port 8001,  8 SMs) ── 0.30 util ── 36.8 GiB (7.35 GB NVFP4 Weights + 29.45 GB KV Cache)
├── RapidAlert Backend (uvicorn / DINOv2) ────────────── 21.0 GiB (Feature Extractor + 7× NVMM Ring Buffers)
├── Desktop, GNOME Shell, Xorg & System Services ──────── 8.0 GiB (Display & Windowing Subsystems)
└── Uncommitted Safety Headroom ───────────────────────── 16.5 GiB (Burst Concurrency Buffer & OOM Shield)
```

### Dynamic GPU Activity Calculation (NVML Power Curve)
Under Jetson Thor MIG mode, traditional static driver utilization queries return discrete sub-partition metrics. RapidAlert implements an **NVML Dynamic Power-Curve Engine** (`backend/services/metrics_monitor.py`):
$$\text{Activity } (\%) = \min\left(100, \max\left(0, \frac{\text{Power}_{\text{NVML}} - P_{\text{idle}}}{P_{\text{max}} - P_{\text{idle}}} \times 100\right)\right)$$
* $P_{\text{idle}} = 18.0\text{ W}$ (Baseline idle platform power).
* $P_{\text{max}} = 120.0\text{ W}$ (Full compute saturation under Dual-MIG parallel inference).

---

## 🧩 NVIDIA Thor MIG Partitioning Deepdive

### Supported MIG Profiles on NVIDIA Thor (Official JetPack 7.2 Matrix)

NVIDIA Jetson AGX Thor features **20 physical Streaming Multiprocessors (SMs)** on Blackwell Tegra silicon, partitioned across a 3-slice hardware architecture. The system supports seven distinct MIG profiles:

| Deployment Status | Profile ID | Profile Name | Slices | Compute SMs | Graphics Engine | Media Engine | Description & Target Workload |
| :---: | :---: | :--- | :---: | :---: | :---: | :---: | :--- |
| 🟢 **Active (Shard 1)** | **78** | `MIG 1g.0gb+me` | 1 | 6 SMs | No | **Yes** (NVENC/DEC/OFA/JPEG) | 1-slice configuration with engine/media tasks. Dedicated video reasoning (**vLLM Shard 1**, Port 8001). |
| 🔄 *Fallback (Shard 1)* | **80** | `MIG 1g.0gb` | 1 | 6 SMs | No | No | 1-slice plain compute instance (automated fallback if Profile 78 is locked). |
| ⚪ *Supported* | **81** | `MIG 1g.0gb+gfx` | 1 | 6 SMs | **Yes** (3D GFX) | No | 1-slice compute instance with 3D graphics support for embedded displays. |
| ⚪ *Supported* | **82** | `MIG 2g.0gb` | 2 | 12 SMs | No | No | 2-slice plain compute instance. Headless high-throughput CUDA compute. |
| 🟢 **Active (Shard 0)** | **83** | `MIG 2g.0gb+gfx` | 2 | 12 SMs | **Yes** (3D GFX + Display) | No | Display/desktop, DINOv2 feature extraction & primary VLM reasoning (**vLLM Shard 0**, Port 8000). |
| ⚪ *Supported* | **0** | `MIG 3g.0gb` | 3 | 20 SMs | No | No | 3-slice full plain compute instance. Headless massive-scale unified compute (100% silicon). |
| ⚪ *Supported* | **32** | `MIG 3g.0gb+gfx` | 3 | 20 SMs | **Yes** (3D GFX + Display) | No | 3-slice full compute instance with graphics support. Full silicon utilization with desktop display. |

### Optimal Dual-MIG Production Pairing (Profile 83 + Profile 78)
On NVIDIA Jetson AGX Thor, the production workload is sharded into two hardware-isolated slices totaling all 20 SMs:
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       NVIDIA AGX THOR SILICON (20 SMs)                      │
├──────────────────────────────────────────────┬──────────────────────────────┤
│       Profile 83: MIG 2g.0gb+gfx (12 SMs)    │ Profile 78: MIG 1g.0gb+me (8 SMs) │
├──────────────────────────────────────────────┼──────────────────────────────┤
│ • 12 CUDA Streaming Multiprocessors         │ • 6 Active CUDA Compute SMs  │
│ • 3D Graphics & Vulkan/OpenGL Engine         │ • 2 Hardware Media Controllers│
│ • Display Engine / Xorg / Desktop Attached   │ • 1× NVDEC (Hardware 4K Dec) │
│ • vLLM Instance 0 (Port 8000)                │ • 1× NVENC (Hardware 4K Enc) │
│ • RapidAlert Backend (DINOv2 + NVMM)        │ • 1× OFA (Optical Flow Accel)│
│                                              │ • 1× JPEG Hardware Decoder   │
│                                              │ • vLLM Instance 1 (Port 8001)│
└──────────────────────────────────────────────┴──────────────────────────────┘
```
* **Slice 0 (Profile 83 / `2g.0gb+gfx`)**: 12 SMs compute + Display/Graphics. Houses desktop windowing, DINOv2 real-time feature extraction, and `rapidalert_vllm_0` on port 8000 (`weight=3`).
* **Slice 1 (Profile 78 / `1g.0gb+me`)**: 6 SMs compute + 2 dedicated media controllers. Houses `rapidalert_vllm_1` on port 8001 (`weight=2`) with hardware-accelerated video ingestion.
* **Automatic Fallback**: If Profile 78 is unavailable, the system safely falls back to Profile 83 + Profile 80 (`1g.0gb`).

### Automated Partitioning Script (`scripts/create-thor-mig.sh`)
RapidAlert provisions and verifies MIG partitions automatically on boot:
* Verifies driver availability and persistence mode (`nvidia-smi -pm 1`).
* Enables MIG mode on GPU 0 (`nvidia-smi -i 0 -mig 1`).
* Verifies both GPU Instances (`COUNT >= 2`) and Compute Instances (`CI_COUNT >= 2`), repairing missing compute instances dynamically.
* Executes batch slice creation: `nvidia-smi mig -i 0 -cgi 83,78 -C`.
* Sets permissions on capability devices: `chmod 666 /dev/nvidia-caps/nvidia-cap*`.
* Auto-syncs discovered MIG UUIDs directly into `config/system.json`.

### Container Device Interface (CDI) & Device Capabilities (`/dev/nvidia-caps`)
On JetPack 7.2 CDI mode, Docker containers cannot access GPUs via standard desktop flags. Instead, direct **MIG Capability Nodes** must be mounted:
* **Universal Root Configuration Node (`nvidia-cap1`)**: **Mandatory for ALL containers** running under MIG. Omitting `/dev/nvidia-caps/nvidia-cap1` causes `cudaErrorNoDevice` or initialization failure.
* **Global Monitor Node (`nvidia-cap2`)**: Global telemetry monitoring capability.
* **Instance 0 (GI 1 / Slice 0)**: Mounts `nvidia-cap1`, `nvidia-cap12`, and `nvidia-cap13`.
* **Instance 1 (GI 2 / Slice 1)**: Mounts `nvidia-cap1`, `nvidia-cap21`, and `nvidia-cap22`.

---

## 🔄 Autonomous Scheduler & Follow-Up Lifecycle

RapidAlert's scheduler (`backend/services/scheduler.py`) unifies real-time event routing with autonomous hazard tracking:

### Event-Driven Priority Assignment
* **Priority 0 (Critical)**: Active incident follow-ups or major visual drift exceeding `dino_major_threshold` (0.033).
* **Priority 1 (Standard)**: Minor scene shifts exceeding `dino_minor_threshold` (0.0165).
* **Priority 3 (Background)**: Periodic ambient heartbeats (35-second cadence).

### Autonomous Follow-Up Loop (High/Danger Persistence)
When Cosmos Reason2 classifies an incident as **`HIGH`** or **`DANGER`**:
1. An autonomous follow-up loop is instantiated for that camera stream.
2. Re-evaluates the camera every **10 seconds** (configurable via `followup_interval_sec`) for up to **6 consecutive cycles** (`followup_max_cycles`).
3. Follow-up requests enter Tier A at **Priority 0**, preempting routine background analyses until the safety hazard has officially resolved or concluded.

---

## 🛡️ Automated Health Watchdog & Real-Time Alerting

RapidAlert runs an autonomous health sentinel (`backend/services/watchdog.py`) alongside a dedicated container supervisor (`backend/services/container_manager.py`) and zero-lag async email dispatcher (`backend/services/watchdog_emailer.py`).

### Automated Container Crash Watchdog & Self-Healing Engine
Every **15 seconds**, the watchdog audits all running vLLM inference containers:
* **Defect Inspection**: Inspects `docker inspect` for `State.Status` (`running`, `exited`, `dead`), non-zero exit codes (e.g. exit 137 = OOM, exit 139 = SIGSEGV), and HTTP `/health` probe responsiveness.
* **Automated Self-Healing**:
  * Tries an in-place `docker restart` with a 10-second grace period.
  * If the container is missing or corrupted, recreates the container with full Thor CDI capabilities (`/dev/nvidia-caps/nvidia-cap1`, `nvidia-cap${GI}*`, `--runtime nvidia`, `--network host`).
* **Audit Logging**: Every remediation event is written to SQLite table `watchdog_sanitation_log`.
* **Resolution Verification**: Probes container health until HTTP 200 is confirmed, then clears active fault tracking and dispatches a green `[RESOLVED]` notice.

### Crash-Loop Backoff & Anti-Thrashing Guardrails
To prevent perpetual restart storms during catastrophic model weight corruption or unrecoverable CUDA faults:
* Restarts are tracked within a sliding 10-minute window (`cooldown_seconds = 600`).
* **Rate-Limit Guardrail**: **Maximum 3 restarts** per 10-minute window.
* If limit is breached, auto-restart halts, the container is flagged `BACKOFF_EXCEEDED`, and an emergency alert is dispatched to administrators for manual intervention.

### Operator Diagnostic Solving Tools & Incident Emails
When an inference container crashes or stalls, `WatchdogEmailer.send_container_crash_alert()` dispatches a comprehensive HTML diagnostic report containing:
1. **Defect Breakdown**: Container name, exit code, failure category (`OOM Killed`, `Process Crashed`, `Endpoint Unresponsive`).
2. **Host Resource Snapshot**: Free physical RAM, swap usage, disk availability, and Jetson Thor GPU wattage.
3. **Trailing Monospace Container Logs**: The last 40 lines of container `stdout`/`stderr`.
4. **Actionable Copy-Paste Operator Solving Tools**:
   ```bash
   # 1. View live trailing container logs
   docker logs --tail 100 -f rapidalert_vllm_0

   # 2. Inspect container lifecycle state
   docker inspect rapidalert_vllm_0 | jq '.[0].State'

   # 3. Perform manual container restart
   docker restart rapidalert_vllm_0

   # 4. Probe inference endpoint health
   curl -s http://localhost:8000/health

   # 5. Verify Jetson Thor MIG partition status
   nvidia-smi mig -lgi

   # 6. Trigger immediate Watchdog audit via REST
   curl -X POST http://localhost:7000/api/watchdog/audit-containers
   ```

### Real-Time Critical Incident Alerting Criteria
Immediate critical incident emails are dispatched via Gmail SMTP (`reportsclove@gmail.com`) upon detection of:
1. **`SYSTEM_RESTART` / Crash Recovery**: System reboots or unexpected crash recoveries detected on startup.
2. **`CONTAINER_CRASH`**: vLLM container crash, OOM kill, or unresponsive endpoint with automated healing action.
3. **`CAMERA_BLACKOUT`**: More than 50% or all camera feeds disconnected/frozen for $>30\text{ seconds}$.
4. **`VLLM_CLUSTER_DOWN`**: All partitioned MIG inference shards offline or failing HTTP health checks.
5. **`STORAGE_CRITICAL`**: Disk free space falls below $5\%$ or SQLite database corruption occurs.
6. **`FATAL_EXCEPTION`**: Unhandled critical exceptions in core video ingestion or analysis loops.

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
| `POST` | `/api/watchdog/audit-containers` | Trigger manual watchdog container health audit & auto-healing | Yes (Admin) |
| `POST` | `/api/watchdog/test-alert` | Dispatch a test critical incident alert email | Yes (Admin) |
| `POST` | `/api/watchdog/send-daily-digest` | Force compile & dispatch 24h health digest | Yes (Admin) |
| `GET` | `/api/vlm/containers` | Live status, exit codes, OOM flags, and resource stats of vLLM containers | No |
| `POST` | `/api/vlm/containers/restart/{name}` | Operator manual container restart (bypassing backoff limit) | Yes (Admin) |
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
