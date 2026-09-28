# RapidAlert: Architectural & Technical Deep Dive
### Engineering Reference: Hybrid Edge VLM Surveillance on NVIDIA Jetson AGX Thor

---

## 1. System Philosophy & Executive Summary

Traditional CCTV computer vision relies on narrow object detection (e.g. YOLO bounding boxes) or basic optical flow thresholds that lack semantic context, causal reasoning, and temporal continuity. Conversely, cloud-hosted Multimodal Large Language Models (MLLMs) introduce multi-second network latencies, bandwidth saturation from multi-camera feeds, and critical data sovereignty liabilities.

**RapidAlert** resolves this paradigm through an **Edge-Native Hybrid Vision Pipeline**:

1. **Tier 1 (Sub-10ms Fast Path)**: Real-time dense feature extraction via **DINOv2** (`facebook/dinov2-small`) computes continuous cosine embedding drift on local frames to detect any structural or semantic shift with near-zero compute overhead.
2. **Tier 2 (Deep Multimodal Reasoning Path)**: When a shift exceeds calibrated thresholds, a multi-tier deadline scheduler captures a **4-frame temporal sequence** ($t-10s, t-6.5s, t-3s, t-0s$) and routes it to **Cosmos Reason2 8B (NVFP4)** running across hardware-isolated **NVIDIA Multi-Instance GPU (MIG)** compute shards.
3. **Tier 3 (Direct Hardware Streaming & Active-Page Viewport)**: High-resolution camera feeds bypass Python CPU image compression entirely through an **Embedded MediaMTX Gateway**, delivering direct Low-Latency HLS (LL-HLS) to HTML5 `<video>` elements decoded by Chromium's GPU hardware video engine.
4. **Tier 4 (Autonomous Persistence & Follow-Up)**: If a safety hazard or critical incident is flagged (`HIGH`/`DANGER`), an autonomous follow-up loop re-inspects the scene every 10 seconds until the situation resolves.
5. **Tier 5 (Continuous Watchdog & Shift Reporting)**: A continuous hardware sentinel tracks power cuts, kernel crashes, camera blackouts, and cluster outages, dispatching real-time email alerts and automated 24-hour health digests alongside scheduled shift PDF reports stored in a rolling 69-report archive.

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
└──────────────────────────────┴───────────────┴──────────────────────────────┘
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

## 3. Direct Hardware RTSP Streaming & Low-Latency HLS Architecture

Streaming 7 high-definition CCTV cameras over standard HTTP multipart MJPEG requires continuous server-side JPEG compression, consuming upwards of 200–400% CPU on multi-core systems and introducing frame lag. RapidAlert bypasses this through an **Embedded MediaMTX Hardware Passthrough Architecture**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 DIRECT HARDWARE VIDEO INGESTION & PLAYBACK                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. IP Cameras (H.264/H.265 RTSP) ──> MediaMTX Daemon (Port 8888 / 8889)    │
│ 2. MediaMTX Repackages RTSP into Low-Latency HLS fMP4 Chunks (200ms parts)  │
│ 3. Frontend HTML5 <video> + Hls.js Pulls Chunks via HTTP                    │
│ 4. Chromium GPU Video Decoder (VDPAU / NVDEC) Decodes Directly to Display   │
│                                                                             │
│ Result: 0% Server CPU Transcoding | Native 30/60 FPS | Sub-Second Latency   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 MediaMTX Daemon Lifecycle & Dynamic Configuration
* **Supervision**: `backend/services/mediamtx_service.py` dynamically writes `stream/mediamtx.yml` based on active cameras in `config/cameras.json`.
* **Port Allocations**:
  - `8888`: Low-Latency HLS (LL-HLS) endpoint (`http://localhost:8888/{slug}/index.m3u8`).
  - `8889`: WebRTC / WHEP endpoint (`http://localhost:8889/{slug}/whep`).
  - `8554`: Local RTSP proxy.
  - `9997`: MediaMTX control API.

### 3.2 Active-Page Pagination & Zero-Overhead Viewport Policy
* **Pagination Bounds**:
  - In $2\times2$ mode (4 cameras/page), only the 4 cameras on the current page have active media sockets.
  - In $3\times2$ mode (6 cameras/page), only the 6 cameras on the current page are connected.
* **Socket Teardown on Page Switch**:
  - When switching pages (e.g. Page 1 $\leftrightarrow$ Page 2), `_detachDirectStream(name)` immediately calls `hls.destroy()` and unloads the `<video>` element, freeing browser socket pools and network bandwidth.
* **Thread-Safe Fallback Pipeline (`FrameStore`)**:
  - If a browser lacks HLS support or during stream initialization, `FrameStore` provides an optimized MJPEG stream using a `threading.Condition` and monotonic sequence numbers (`cur_seq`), waking waiting threads in $<0.1\text{ms}$ upon new NVDEC frame decode.

---

## 4. Multi-Instance GPU (MIG) & Container Virtualization

### 4.1 Partition Profiles on JetPack 7.2

Under JetPack 7.2 for Jetson AGX Thor, hardware partitioning is established via `/usr/local/bin/create-thor-mig.sh`:

1. **Slice 0 (Profile 83 / `2g.0gb+gfx`)**:
   * **12 Streaming Multiprocessors (SMs)**.
   * Graphics-enabled: Binds Xorg, GNOME Shell, and Desktop UI rendering alongside `vllm_0` (Port 8000).
2. **Slice 1 (Profile 78 / `1g.0gb+me`)**:
   * **8 Physical SMs** (6 general-purpose CUDA Compute SMs + 2 SMs dedicated to NVDEC, NVENC, OFA, and JPEG engines).
   * Houses `vllm_1` (Port 8001) for dedicated video reasoning.

### 4.2 Jetson CDI Capability Mapping Architecture

Standard desktop Docker flags like `NVIDIA_VISIBLE_DEVICES=MIG-...` fail on Jetson's Container Device Interface (CDI) in CSV mode. RapidAlert mounts the specific **MIG Capability Nodes** directly into the container namespace:

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

## 5. Two-Tier In-Memory Queuing & Scheduling System

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

### 5.1 Tier A: Global Priority Router
* Tasks are prioritized into `asyncio.PriorityQueue` tuples: `(priority, timestamp, task)`.
* Priority ordering ensures that ongoing critical follow-ups (Priority 0) preempt minor background scene shifts (Priority 1) or ambient heartbeats (Priority 3).

### 5.2 Tier B: Weighted Least-Connections Router
The router selects the optimal MIG shard using a weighted load score:
$$\text{Load Score} = \frac{\text{In-Flight Requests} + \text{Queued Tasks}}{\text{Weight}_{\text{shard}}}$$
* Shard 0 (12 SMs) has `weight = 3` (capacity for 4 concurrent requests).
* Shard 1 (8 SMs) has `weight = 2` (capacity for 4 concurrent requests).

---

## 6. Automated Watchdog Sentinel & 24h Diagnostic Digest

### 6.1 Real-Time Critical Incident Dispatch
The `WatchdogEmailer` service (`backend/services/watchdog_emailer.py`) executes non-blocking asynchronous email notifications via Gmail SMTP:
* **`SYSTEM_RESTART`**: Notifies administrators on startup, distinguishing between clean reboots and unexpected crash recoveries.
* **`CAMERA_BLACKOUT`**: Triggers if $>50\%$ of feeds freeze or drop for $>30\text{ seconds}$.
* **`VLLM_CLUSTER_DOWN`**: Triggers if both MIG inference endpoints fail health checks.
* **`STORAGE_CRITICAL`**: Triggers if disk capacity drops below $5\%$ or SQLite corruption is detected.
* **`FATAL_EXCEPTION`**: Triggers upon unhandled pipeline crashes.

### 6.2 Automatic Resolution Notices
When active fault conditions clear, the watchdog automatically dispatches a green `[RESOLVED]` notice, resetting cooldown trackers.

### 6.3 Executive 24-Hour System Health Digest
* **Trigger Schedule**: Automated at **08:00 AM IST** daily.
* **SQL Aggregation**: Compiles total uptime, completed analyses, error log severities, NVDEC vs CPU decoding ratios, and MIG shard latencies from `data/analyses.db`.

---

## 7. Shift Intelligence Reporting & 69-Report Rolling Buffer

### 7.1 Automated Shift Schedule
`ReportingService` (`backend/services/reporting_service.py`) generates multi-page executive shift intelligence PDFs at:
* **06:00 AM IST**: Concluding the Night Shift.
* **06:00 PM IST**: Concluding the Day Shift.

### 7.2 Circular Buffer Management
* Stored in `data/reports/` with naming convention `shift_report_YYYYMMDD_HHMMSS.pdf`.
* Retains a rolling buffer of **69 reports** (~1 month of operational shift history).
* When report #70 is generated, the oldest report is automatically purged.
* Operators can inspect, view, and download all 69 past reports via the **Reports Archive Modal** on the dashboard.

---

## 8. Summary of Performance Metrics

| Subsystem | Metric | Jetson AGX Thor Performance |
| :--- | :--- | :--- |
| **DINOv2 Feature Extractor** | Latency per frame | **8.5 ms** (CPU / CUDA) |
| **Cosmos Reason2 8B Inference** | 4-Frame Temporal Latency | **1.35 s – 1.85 s** (NVFP4 on MIG) |
| **Direct Hardware HLS Stream** | Client-side playback | **30 FPS** @ 0% Server CPU |
| **MIG Ingestion Parallelism** | Concurrent VLM Streams | **8 Active Requests** (4 per MIG shard) |
| **Camera Ingest Capacity** | Stream Concurrency | **7× 1080p Streams** simultaneously |
| **Watchdog Alert Dispatch** | Mean Notification Delay | **< 1.5 s** via Async SMTP |
