# RapidAlert: Architectural & Technical Deep Dive
### Engineering Reference: Hybrid Edge VLM Surveillance on NVIDIA Jetson AGX Thor

---

## 1. System Philosophy & Executive Summary

Traditional CCTV computer vision relies on narrow object detection (YOLO) or optical flow bounding boxes that lack semantic context, causal reasoning, and temporal understanding. Conversely, cloud-hosted Multimodal Large Language Models (MLLMs) introduce multi-second network latencies, bandwidth saturation from high-resolution multi-camera feeds, and critical data sovereignty liabilities.

**RapidAlert** resolves this paradigm through an **Edge-Native Hybrid Vision Pipeline**:
1. **Tier 1 (Sub-10ms Fast Path)**: Real-time dense feature extraction via **DINOv2** (`facebook/dinov2-small`) computes continuous cosine embedding drift on local frames to detect any structural or semantic shift with near-zero compute overhead.
2. **Tier 2 (Deep Multimodal Reasoning Path)**: When a shift exceeds calibrated thresholds, a multi-tier deadline scheduler captures a **4-frame temporal sequence** ($t-10s, t-6.5s, t-3s, t-0s$) and routes it to **Cosmos Reason2 8B (NVFP4)** running across hardware-isolated **NVIDIA Multi-Instance GPU (MIG)** compute shards.
3. **Tier 3 (Autonomous Persistence & Follow-Up)**: If a safety hazard or critical incident is flagged (`HIGH`/`DANGER`), an autonomous follow-up loop re-inspects the scene every 10 seconds until the situation resolves.
4. **Tier 4 (Continuous Watchdog & Reporting)**: A continuous hardware sentinel tracks power cuts, kernel crashes, camera blackouts, and cluster outages, dispatching real-time email alerts and automated 24-hour health digests alongside scheduled shift PDF reports.

---

## 2. Hardware Architecture & Unified Memory Subsystem

### 2.1 NVIDIA Jetson AGX Thor Platform Specifications

* **Compute Silicon**: NVIDIA Blackwell Architecture with 4th Gen Tensor Cores & NVFP4 Tensor Accelerators.
* **Streaming Multiprocessors (SMs)**: **20 SMs** in physical silicon.
* **Unified Memory Subsystem**: **128 GB 256-bit LPDDR5X Unified Memory** operating at 4266 MHz (over 200 GB/s bandwidth).
* **Hardware Media Accelerators**: 
  - NVDEC (Hardware 4K H.264/H.265/AV1 Decoder Engine)
  - NVENC (Hardware 4K Real-Time Encoder Engine)
  - OFA (Optical Flow Hardware Accelerator)
  - Hardware JPEG Decoder Engine

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

### 2.2 Unified Memory Mechanics & Allocation Topology

On NVIDIA Thor, **Host RAM and GPU VRAM share the same physical silicon address space**. Every CUDA tensor, frame buffer, and PagedAttention KV cache block directly claims physical LPDDR5X system RAM.

#### Memory Sizing Formula in vLLM:
The physical memory claimed by each vLLM instance on startup is governed by:
$$\text{Memory}_{\text{vLLM}} = \text{Weights}_{\text{NVFP4}} + \left( \text{RAM}_{\text{total}} \times \text{gpu\_memory\_utilization} \right)$$

Where:
* $\text{Weights}_{\text{NVFP4}} \approx 7.35\text{ GiB}$ for `vrfai/Cosmos-Reason2-8B-NVFP4`.
* $\text{RAM}_{\text{total}} = 122.8\text{ GiB}$ (Usable LPDDR5X capacity).

#### Production Memory Budget:
```
Total Unified Physical Memory: 122.8 GiB (128 GB)
├── vLLM Instance 0 (Port 8000, 12 SMs) ── 0.33 util ── 40.5 GiB (7.35 GB Weights + 33.15 GB KV Cache)
├── vLLM Instance 1 (Port 8001,  8 SMs) ── 0.30 util ── 36.8 GiB (7.35 GB Weights + 29.45 GB KV Cache)
├── RapidAlert Backend (uvicorn / DINOv2) ────────────── 21.0 GiB (Embedding Model + 7× NVMM Frame Buffers)
├── Desktop, GNOME Shell, Xorg & IDE ──────────────────── 8.0 GiB (Display Server & Dev Tools)
└── Uncommitted Safety Headroom ───────────────────────── 16.5 GiB (Burst Concurrency & Zero-OOM Buffer)
```

### 2.3 Dynamic NVML Power-Curve GPU Utilization Engine
Under Jetson Thor MIG mode, traditional static driver utilization queries return discrete sub-partition metrics. RapidAlert implements a unified **NVML Dynamic Power-Curve Model** (`backend/services/metrics_monitor.py`):
$$\text{Activity } (\%) = \min\left(100, \max\left(0, \frac{\text{Power}_{\text{NVML}} - P_{\text{idle}}}{P_{\text{max}} - P_{\text{idle}}} \times 100\right)\right)$$
* $P_{\text{idle}} = 18.0\text{ W}$ (Baseline idle platform power).
* $P_{\text{max}} = 120.0\text{ W}$ (Full compute saturation under Dual-MIG parallel inference).

---

## 3. Multi-Instance GPU (MIG) & Container Virtualization

### 3.1 Partition Profiles on JetPack 7.2

Under JetPack 7.2 for Jetson AGX Thor, hardware partitioning is established prior to display manager initialization via `/usr/local/bin/create-thor-mig.sh`:

1. **Slice 0 (Profile 83 / `2g.0gb+gfx`)**:
   * **12 Streaming Multiprocessors (SMs)**.
   * Graphics-enabled: Binds Xorg, GNOME Shell, and Desktop UI rendering alongside `vllm_0` (Port 8000).
2. **Slice 1 (Profile 78 / `1g.0gb+me`)**:
   * **8 Physical SMs** (6 general-purpose CUDA Compute SMs + 2 SMs dedicated to NVDEC, NVENC, OFA, and JPEG engines).
   * Houses `vllm_1` (Port 8001) for dedicated video reasoning.

### 3.2 Jetson CDI Capability Mapping Architecture

Standard desktop Docker flags like `NVIDIA_VISIBLE_DEVICES=MIG-...` fail on Jetson's Container Device Interface (CDI) in CSV mode. RapidAlert resolves this by mounting the specific **MIG Capability Nodes** directly into the container namespace:

```bash
# Instance 0 (GI 1): Maps cap1, cap12, cap13
docker run -d \
    --name "rapidalert_vllm_0" \
    --runtime nvidia \
    --network host \
    --shm-size=4g \
    -e NVIDIA_VISIBLE_DEVICES=all \
    -e CUDA_VISIBLE_DEVICES=0 \
    --device /dev/nvidia-caps/nvidia-cap1 \
    --device /dev/nvidia-caps/nvidia-cap12 \
    --device /dev/nvidia-caps/nvidia-cap13 \
    ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor \
    vllm serve "vrfai/Cosmos-Reason2-8B-NVFP4" --port 8000 --gpu-memory-utilization 0.33 ...

# Instance 1 (GI 2): Maps cap2, cap21, cap22
docker run -d \
    --name "rapidalert_vllm_1" \
    --runtime nvidia \
    --network host \
    --shm-size=4g \
    -e NVIDIA_VISIBLE_DEVICES=all \
    -e CUDA_VISIBLE_DEVICES=0 \
    --device /dev/nvidia-caps/nvidia-cap2 \
    --device /dev/nvidia-caps/nvidia-cap21 \
    --device /dev/nvidia-caps/nvidia-cap22 \
    ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor \
    vllm serve "vrfai/Cosmos-Reason2-8B-NVFP4" --port 8001 --gpu-memory-utilization 0.30 ...
```

---

## 4. Ingestion Pipeline: DeepStream NVDEC & FrameStore

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HIGH-THROUGHPUT NVDEC INGESTION PIPELINE                 │
├───────────────┬───────────────────┬───────────────────┬─────────────────────┤
│  RTSP Stream  │ Hardware Decoder  │  Format Transform │ Thread-Safe Storage │
│ (H.264/H.265) │   (nvurisrcbin)   │ (nvvideoconvert)  │  (Ring FrameStore)  │
├───────────────┼───────────────────┼───────────────────┼─────────────────────┤
│ 7 Streams @   │ Zero-Copy NVMM    │ Hardware NVMM to  │ Rolling circular    │
│ 1080p / 4K    │ Hardware Decoder  │ BGR/RGB 768px max │ buffer of 4 frames  │
│ 25-30 FPS     │ 0% CPU footprint  │ with 0-copy DMA   │ with timestamps     │
└───────────────┴───────────────────┴───────────────────┴─────────────────────┘
```

### 4.1 Zero-Staleness Frame Ingestion & CPU Fallback
In real-time VLM surveillance, inference takes **1.2–2.0 seconds**, while cameras produce frames every **33 ms** (30 FPS). Traditional FIFO queues inevitably back up, causing models to analyze stale history.

RapidAlert implements a **Non-Blocking Ring Buffer (`FrameStore`)**:
* Dedicated ingestion threads poll hardware NVDEC decoders (`nvurisrcbin`).
* If an NVDEC bufferpool error or corrupt RTSP keyframe sequence occurs, the camera manager dynamically fails over to a high-speed **CPU OpenCV fallback engine**, preventing stream drops.
* As frames arrive, they overwrite a fixed-capacity ring buffer ($t-10s, t-6.5s, t-3s, t-0s$).
* When an inference trigger fires, the scheduler immediately extracts the exact $4$-frame temporal history without queuing delay.

---

## 5. Dual-Tier Scene Shift Intelligence

```mermaid
flowchart LR
    FRAME["Live Camera Frame (t-0)"] --> DINO["DINOv2 Feature Extractor"]
    DINO --> EMBED["384-Dim Embedding Vector E(t)"]
    EMBED --> DRIFT["Cosine Distance vs Baseline E(0)"]
    DRIFT --> EVAL{"Drift Value"}
    EVAL -->|">= 0.060"| MAJOR["🚨 Major Shift (P1)<br>Tier-2 Emergency VLM"]
    EVAL -->|">= 0.030"| MINOR["⚠️ Minor Shift (P1)<br>Tier-3 Scene Shift VLM"]
    EVAL -->|"< 0.030 & > 35s"| HB["💓 Periodic Audit (P3)<br>Tier-4 Heartbeat VLM"]
```

### 5.1 Cosine Distance Metric
$$\mathcal{D}_{\text{drift}}(E_t, E_{\text{baseline}}) = 1 - \frac{\sum_{i=1}^{384} E_{t,i} \cdot E_{\text{baseline},i}}{\sqrt{\sum_{i=1}^{384} E_{t,i}^2} \cdot \sqrt{\sum_{i=1}^{384} E_{\text{baseline},i}^2}}$$

### 5.2 Dynamic Drift Adaptation
To handle natural lighting shifts (e.g. dawn, dusk, cloud cover) without triggering false alarms, the baseline embedding is updated via an exponential moving average (EMA) filter:
$$E_{\text{baseline}} \leftarrow (1 - \alpha) E_{\text{baseline}} + \alpha E_t \quad (\alpha = 0.02)$$

When an actual incident occurs ($\mathcal{D}_{\text{drift}} \ge \text{threshold}$), EMA adaptation is temporarily frozen to prevent the anomaly from poisoning the reference baseline.

---

## 6. Multi-Frame Temporal Reasoning Engine

### 6.1 Interleaved Multimodal Token Representation
When a trigger fires, RapidAlert constructs a multi-image payload with explicit temporal frame markers:

```
[SYSTEM PROMPT]
You are RapidAlert Edge Reasoning Engine running on NVIDIA Jetson AGX Thor.
Analyze the 4-frame temporal sequence below. Frames are ordered sequentially from t-3 to t-0.

[FRAME 1: t -10.0s (Baseline Context)]
<image_token_1>

[FRAME 2: t -6.5s (Pre-Incident Initiation)]
<image_token_2>

[FRAME 3: t -3.0s (Incident Development)]
<image_token_3>

[FRAME 4: t 0.0s (Current Trigger State)]
<image_token_4>

[INSTRUCTION]
1. State exactly what changed between t-3 and t-0.
2. Identify actors (workers, vehicles, intruders).
3. Classify severity: LOW, MEDIUM, or HIGH.
4. Classify safety hazard: OK, WARNING, or DANGER.
5. Return strictly valid JSON.
```

---

## 7. Two-Tier In-Memory Queuing & Dispatch System

```
                        [ DINOv2 / Timer / API Events ]
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │     Tier A: Global Dispatch Priority Queue (PriorityQueue)          │
    │     • Priority 1: Instant Scene Shift & Trigger Events (P1)         │
    │     • Priority 2: 10s Adaptive Follow-up Verification (P2)          │
    │     • Priority 3: Routine Ambient Heartbeat Audits (P3)             │
    └──────────────────────────────────┬──────────────────────────────────┘
                                       │  Deadlock Prevention: In-Flight Lock Set
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │     Weighted Least-Connections Load Balancer (MIGAwareVLMPool)      │
    │                                                                     │
    │                 load_score = (in_flight + queued) / weight          │
    └──────────────────┬───────────────────────────────┬──────────────────┘
                       │ (weight=3)                    │ (weight=2)
                       ▼                               ▼
    ┌────────────────────────────────────┐ ┌────────────────────────────────────┐
    │ Tier B1: Shard 0 Bounded Queue     │ │ Tier B2: Shard 1 Bounded Queue     │
    │ • asyncio.Queue (maxsize=12)       │ │ • asyncio.Queue (maxsize=12)       │
    │ • asyncio.Semaphore (max=4)        │ │ • asyncio.Semaphore (max=4)        │
    │ • 4 Coroutine Workers (Port 8000)  │ │ • 4 Coroutine Workers (Port 8001)  │
    └────────────────────────────────────┘ └────────────────────────────────────┘
```

### 7.1 Tier A: Global Dispatch Priority Queue (`PriorityQueue`)
* **Underlying Implementation**: `asyncio.PriorityQueue` storing tuples of `(priority_int, timestamp, job_payload)`.
* **Zero-IPC Latency**: Eliminates network message broker hops (Redis/RabbitMQ), operating purely in Python memory.
* **Priority Tiers**:
  * **Priority 1 (`TRIGGER`)**: Emergency scene shifts triggered by DINOv2 major/minor threshold breach.
  * **Priority 2 (`FOLLOWUP`)**: 10-second adaptive verification cycle for ongoing hazard tracking.
  * **Priority 3 (`PERIODIC / HEARTBEAT`)**: Background ambient baseline health checks every 35s.

### 7.2 Tier B: Per-MIG Endpoint Worker Shard Queues (`_EndpointShard`)
* **Bounded Queues**: `asyncio.Queue(maxsize=queue_depth)` per shard enforces strict backpressure.
* **Concurrency Cap**: `asyncio.Semaphore(max_concurrent=4)` prevents overloading the vLLM KV-cache.
* **Weighted Load Score Formula**:
  $$\text{Load Score}_i = \frac{\text{In-Flight Requests}_i + \text{Queue Depth}_i}{\text{Weight}_i}$$
  * Shard 0 (12 SMs): $\text{Weight} = 3$ ($60\%$ compute capacity).
  * Shard 1 (8 SMs): $\text{Weight} = 2$ ($40\%$ compute capacity).

### 7.3 Deadlock Prevention & In-Flight Lock Deduplication
* An atomic `_in_flight: set[str]` lock tracks active camera analyses.
* If a new scene shift fires on Camera A while Camera A is already in-flight, the frame timestamp is updated in place without duplicate queue generation, preventing queue stampedes.

### 7.4 Cold-Boot Model Warmup & Self-Healing Circuit Breaker
* During the 60–90s cold-start weight-loading window on boot, `MIGAwareVLMPool` holds requests and returns benign non-crashing payloads (`"verdict": "WARMUP"`).
* Continuous background health probes (every 5s during warmup, 15s during normal operation) automatically transition shards to active routing upon receiving HTTP `200 OK`.

---

## 8. Autonomous Follow-Up Lifecycle

```mermaid
stateDiagram-v2
    [*] --> HealthyMonitoring
    HealthyMonitoring --> IncidentTriggered: DINOv2 Drift >= Threshold
    
    IncidentTriggered --> VLM_Analysis: Multi-Frame Temporal Inference
    
    state VLM_Analysis {
        [*] --> ProcessFrames
        ProcessFrames --> CheckSeverity
    }
    
    CheckSeverity --> LowResolved: Severity == LOW & Safety == OK
    CheckSeverity --> HighHazard: Severity == HIGH | Safety == DANGER
    
    LowResolved --> HealthyMonitoring: Incident Closed & Baseline Reset
    HighHazard --> ScheduleFollowUp: Spawn Persistent Follow-Up (P2)
    
    ScheduleFollowUp --> SleepCadence: Wait 10.0 Seconds
    SleepCadence --> VLM_Analysis: Re-evaluate Camera Feed
    
    ScheduleFollowUp --> MaxCyclesReached: Follow-Up Count > 6
    MaxCyclesReached --> HealthyMonitoring: Log Unresolved Incident
```

---

## 9. Continuous Health Watchdog & Real-Time Alerting

RapidAlert implements a multi-layer autonomous alerting and diagnostic engine ([`watchdog_emailer.py`](file:///home/clove/RapidAlert/backend/services/watchdog_emailer.py) & [`watchdog.py`](file:///home/clove/RapidAlert/backend/services/watchdog.py)):

### 9.1 Real-Time Critical Incident Criteria
Immediate executive HTML emails are dispatched with anti-spam deduplication (10-minute cooldown per incident key) and automatic green `[RESOLVED]` emails:

1. **Camera Stream Blackout**: Triggers when $\ge 50\%$ of active feeds have no frame updates for $>15\text{s}$.
2. **vLLM Inference Cluster Down**: Triggers when all MIG instances report unhealthy after the 90s boot grace period.
3. **Storage Critical**: Triggers when host storage drops below $5\%$ free disk space.
4. **Fatal Exceptions**: Hooks into `ErrorTracker.capture_exception` and `capture_error` for `CRITICAL`/`FATAL` events, including full stack trace and caller location.

### 9.2 Hardware Sentinel & Power Outage / Crash Detection
Using a persistent sentinel file (`data/system_state.json`) and Linux kernel boot time inspection (`/proc/uptime`), RapidAlert mathematically detects:

$$\Delta T_{\text{outage}} = T_{\text{boot}} - T_{\text{last\_heartbeat}}$$

* **Power Outage Recovery** ($T_{\text{host\_boot}} > T_{\text{last\_heartbeat}}$): Physical machine suffered hard power loss or reboot.
* **Process Crash Recovery** ($T_{\text{host\_boot}} \le T_{\text{last\_heartbeat}}$ and prior state was `RUNNING`): Unexpected process crash or OOM kill.
* **NVIDIA Thor Host Reboot** (`THOR_SYSTEM_REBOOT`): Clean OS/hardware restart notification.
* **Application Restart** (`SYSTEM_RESTART`): Graceful service restart notification.
* **15-Second Rolling Heartbeat**: `heartbeat_tick()` records live timestamps to ensure sub-minute outage duration precision.

### 9.3 24-Hour System Health & Diagnostics Digest
Triggered automatically every morning at **08:00 AM IST**:
* Summarizes 24h error log frequency (Critical, Error, Warning).
* Tallies total analyses, High hazards, and Medium incidents.
* Details NVDEC vs CPU camera decoder distribution and vLLM shard health.
* Audits PDF shift reports delivered.

---

## 10. Automated Shift Reporting & ReportLab PDF Generation

The reporting engine ([`reporting_service.py`](file:///home/clove/RapidAlert/backend/services/reporting_service.py)) compiles and emails high-resolution PDF intelligence reports:

### 10.1 Shift Boundaries & Delivery
* **Day Shift**: 06:00 AM – 06:00 PM IST (Triggered at 18:00 IST).
* **Night Shift**: 06:00 PM – 06:00 AM IST (Triggered at 06:00 IST).
* **SMTP Delivery**: Asynchronous delivery via Gmail SMTP to `pandalavacarji@gmail.com` and `reportsclove@gmail.com`.

### 10.2 ReportLab Dynamic Auto-Pagination
* Employs dynamic height calculation to budget page space and inject `PageBreak()` elements safely.
* Guarantees zero text truncation, zero visual overlap, and crisp layout rendering.
* Features executive KPI cards, High/Danger incident cards with embedded camera visual evidence, and chronological telemetry logs.

### 10.3 69-Report Rolling Buffer
* Maintains a circular buffer of **69 shift reports** (~**34.5 days**) in `data/reports/`.
* Total disk footprint is under **25 MB**, with automatic FIFO pruning on overflow.

---

## 11. Structured Error Tracking & Downstream Effect Tracing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RAPIDALERT THREE-TIER DEFENSE SYSTEM                     │
├─────────────────────────┬─────────────────────────┬─────────────────────────┤
│  Pre-Flight Auditor     │  Runtime Watchdog       │  Structured Tracker     │
│ (system_health_audit.py)│ (SystemWatchdog Daemon) │ (error_tracker.py)      │
├─────────────────────────┼─────────────────────────┼─────────────────────────┤
│ • Validates all JSON    │ • Checks frame age      │ • Zero silent swallows  │
│ • Reaps zombie procs    │ • Recycles frozen RTSP  │ • Line & component tags │
│ • Tests vLLM endpoints  │ • Non-blocking waitpid  │ • Downstream effect log │
│ • Verifies NVDEC & CUDA │ • Heartbeat probes      │ • WebSocket broadcast   │
└─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

* **Zero Silent Swallowing**: Every exception records Error Type, Message, Caller File & Line, Function Name, Camera Name, and **Downstream Operational Effect**.
* **Dual Persistence**: Memory circular deque (300 records) + SQLite `error_logs` table (WAL mode).
* **Live Broadcast**: Pushes error records to WebSocket clients for instant HUD display.

---

## 12. Frontend Architecture & Multi-Stream Viewport

* **4-Stream Viewport Pagination**: Displays camera cards in a 2×2 grid with pagination pills (`Page 1: 1-4`, `Page 2: 5-7`).
* **Active Stream Optimization**: Off-screen camera streams suspend WebSocket frame decoding, maintaining $< 4\%$ browser CPU usage across 7+ 4K camera streams.
* **Camera Theater Modal**: Instant freeze-frame inspection, chronological incident strips, and dynamic prompt tuning.
* **Shift Reports Modal**: Live report browser, on-demand PDF generation, and in-browser download.

---

## 13. REST API Specification

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/status` | System operational summary, feed health, and vLLM status |
| `GET` | `/api/cameras` | Active camera list and ingestion backend status |
| `GET` | `/api/drifts` | Real-time DINOv2 visual drift scores across all feeds |
| `GET` | `/api/alerts` | Historical incident feed and security alerts |
| `GET` | `/api/errors` | Structured error logs and stack traces |
| `GET` | `/api/errors/summary` | Error aggregation by component, severity, and effect |
| `GET` | `/api/metrics` | Hardware telemetry (GPU power, memory, temperatures) |
| `GET` | `/api/reports` | List of stored shift intelligence PDF reports |
| `GET` | `/api/reports/{id}/pdf`| Download specific shift report PDF |
| `POST` | `/api/reports/generate`| Generate custom shift report on-demand |
| `GET` | `/api/reports/stats` | Reporting schedule, buffer status, and next run time |
| `GET` | `/api/watchdog/status` | Health watchdog telemetry and 24h diagnostic snapshot |
| `POST` | `/api/watchdog/test-alert` | Dispatch a test critical incident alert email (Admin) |
| `POST` | `/api/watchdog/send-daily-digest` | Force compile & dispatch 24h health digest (Admin) |
| `WS` | `/ws` | Real-time binary/JSON telemetry, alerts, and 10 FPS video |

---

## 14. Verification, Logging & Operations

```bash
# Launch RapidAlert
./run.sh

# Graceful Stop
./stop.sh

# Run Pre-Flight System Health Audit
python3 scripts/system_health_audit.py --fix

# Inspect Live Logs
tail -f logs/app.log
tail -f logs/system_events.log
tail -f logs/errors.log
```
