# RapidAlert: Architectural & Technical Deep Dive
### Engineering Reference: Hybrid Edge VLM Surveillance on NVIDIA Jetson AGX Thor

---

## 1. System Philosophy & Executive Summary

Traditional CCTV computer vision relies on narrow object detection (YOLO) or optical flow bounding boxes that lack semantic context, causal reasoning, and temporal understanding. Conversely, cloud-hosted Multimodal Large Language Models (MLLMs) introduce multi-second network latencies, bandwidth saturation from high-resolution multi-camera feeds, and data sovereignty liabilities.

**RapidAlert** resolves this paradigm through an **Edge-Native Hybrid Vision Pipeline**:
1. **Tier 1 (Sub-10ms Fast Path)**: Real-time dense feature extraction via **DINOv2** computes continuous cosine embedding drift on local frames to detect any structural or semantic shift with near-zero compute overhead.
2. **Tier 2 (Deep Reasoning Path)**: When a shift exceeds calibrated thresholds, a multi-tier deadline scheduler captures a **4-frame temporal sequence** ($t-3, t-2, t-1, t-0$) and routes it to **Cosmos Reason2 8B (NVFP4)** running across hardware-isolated **NVIDIA Multi-Instance GPU (MIG)** shards.
3. **Tier 3 (Autonomous Persistence & Follow-Up)**: If a safety hazard or incident is flagged (`HIGH`/`DANGER`), an autonomous follow-up loop re-inspects the scene every 10 seconds until the situation resolves.

---

## 2. Hardware Architecture & Unified Memory Subsystem

### 2.1 NVIDIA Jetson AGX Thor Platform Specifications

* **Compute Capability**: NVIDIA Blackwell Architecture with 4th Gen Tensor Cores & NVFP4 Tensor Accelerators.
* **Streaming Multiprocessors (SMs)**: **20 SMs** in physical silicon.
* **Memory Subsystem**: **128 GB 256-bit LPDDR5X Unified Memory** operating at 4266 MHz (over 200 GB/s memory bandwidth).
* **Hardware Media Accelerators**: NVDEC (Hardware 4K H.264/H.265/AV1 Decoder), NVENC (Hardware Encoder), OFA (Optical Flow Accelerator), JPEG Decoder Engine.

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

### 4.1 Zero-Staleness Frame Ingestion
In real-time VLM surveillance, inference takes **1.2–2.0 seconds**, while cameras produce frames every **33 ms** (30 FPS). Traditional FIFO queues inevitably back up, causing models to analyze stale history.

RapidAlert implements a **Non-Blocking Ring Buffer (`FrameStore`)**:
* Dedicated ingestion threads poll hardware NVDEC decoders.
* As frames arrive, they overwrite a fixed-capacity ring buffer ($t-3, t-2, t-1, t-0$).
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

## 9. Fault Tolerance, Watchdog & Diagnostics

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

### 9.1 Pre-Flight Health Auditor (`scripts/system_health_audit.py`)
* Audits port `7000`, validates JSON schemas (`system.json`, `cameras.json`, `prompts.json`), tests vLLM health, and verifies DeepStream hardware decoder bindings.

### 9.2 Continuous Runtime Watchdog (`backend/services/watchdog.py`)
* Runs every 15 seconds: flags stale cameras ($>15.0\text{s}$), reaps defunct child processes via non-blocking `waitpid`, and audits vLLM endpoint connectivity.

### 9.3 Structured Error Tracker (`backend/core/error_tracker.py`)
* Captures exception types, exact lines of origin, camera contexts, and downstream pipeline effects. Persists structured diagnostics to `data/analyses.db` and `logs/errors.log`.

---

## 10. Frontend Architecture & Multi-Stream Viewport

* **4-Stream Viewport Pagination**: Displays camera cards in a high-performance 2×2 grid with top toolbar pagination pills (`Page 1: 1-4`, `Page 2: 5-7`).
* **Active Stream Optimization**: Off-screen camera streams suspend WebSocket frame decoding, maintaining $< 4\%$ browser CPU usage across 7+ 4K camera streams.
* **Camera Theater Modal**: Enables instant freeze-frame inspection, chronological incident strips, and dynamic prompt tuning.
* **CORS & Multi-Host Networking**: Full cross-origin and LAN connectivity supported out of the box across Wi-Fi (`192.168.1.3:7000`) and Ethernet (`10.91.90.184:7000`).

---

## 11. Configuration Guide & Schema Reference

### `config/system.json`
```json
{
  "vllm_endpoints": [
    {
      "url": "http://localhost:8000",
      "model": "vrfai/Cosmos-Reason2-8B-NVFP4",
      "mig_uuid": "MIG-d08f9290-5142-5a7f-a01a-b5863912a3fa",
      "mig_profile": "2g",
      "sm_count": 12,
      "weight": 3,
      "max_concurrent": 4,
      "queue_depth": 12,
      "gpu_utilization": 0.33
    },
    {
      "url": "http://localhost:8001",
      "model": "vrfai/Cosmos-Reason2-8B-NVFP4",
      "mig_uuid": "MIG-dc038e02-d9bc-54a6-9de6-452fa2b5830f",
      "mig_profile": "1g",
      "sm_count": 8,
      "weight": 2,
      "max_concurrent": 4,
      "queue_depth": 12,
      "gpu_utilization": 0.30
    }
  ],
  "auto_start_vllm": true,
  "vllm_model": "vrfai/Cosmos-Reason2-8B-NVFP4",
  "vllm_port_start": 8000,
  "vllm_max_seqs": 4,
  "vllm_max_model_len": 4096,
  "vllm_gpu_utilization": 0.33,
  "vllm_instances": 2,
  "ingest_backend": "nvidia",
  "trigger_backend": "dinov2",
  "dinov2_model": "facebook/dinov2-small",
  "dino_major_threshold": 0.060,
  "dino_minor_threshold": 0.030,
  "default_heartbeat_sec": 35.0,
  "followup_interval_sec": 10.0,
  "persistent_followup": true,
  "followup_max_cycles": 6
}
```

---

## 12. Verification, Logging & Operations

```bash
# Run RapidAlert
./run.sh

# Gracefully Stop
./stop.sh

# Run Test Suite
python3 tests/test_structure_and_config.py

# Inspect Logs
tail -f logs/app.log
tail -f logs/system_events.log
tail -f logs/errors.log
```
