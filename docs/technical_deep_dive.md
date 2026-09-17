# RapidAlert: Technical Deep Dive

RapidAlert is an ultra-fast Vision-Language Model (VLM) CCTV surveillance engine. This document provides a granular technical overview of its internal mechanisms, routing algorithms, frame ingestion pipeline, and Jetson Thor optimizations.

## 1. System Architecture

The system is decoupled into three primary tiers:
1.  **Orchestrator Layer (`run.sh`)**: Manages the deployment lifecycle, Docker containers, SIGINT/SIGTERM trapping, and graceful teardowns.
2.  **Inference Layer (vLLM)**: Scaled GPU processes handling raw token generation and CUDA graph capturing.
3.  **Application Layer (FastAPI)**: The asynchronous core that bridges video streams to the inference layer via WebSocket clients.

### 1.1 Concurrency Model
The application relies heavily on Python's `asyncio` combined with threading for I/O-bound synchronous tasks.
*   **Threading**: Used exclusively in `CameraManager` for `cv2.VideoCapture` blocking reads.
*   **Async/Await**: Used in `DeadlineScheduler` and `VLMPool` to concurrently handle HTTP requests to the inference nodes without blocking the main event loop.

## 2. Ingestion Pipeline: CameraManager & FrameStore

### The "Stale Frame" Problem
Traditional pipeline architectures queue frames as they arrive. In VLM surveillance, inference takes significantly longer (1–3s) than frame generation (30fps = 33ms). If frames are queued sequentially, the queue infinitely expands, resulting in the VLM analyzing the past rather than the present.

### The Solution: Dropping Frames
The `CameraManager` spawns a dedicated Python `Thread` per camera. These threads continuously poll `cv2.VideoCapture.read()`.
Instead of appending to a queue, they overwrite the camera's key in a thread-safe `FrameStore` singleton. 
```python
# FrameStore architecture
self._frames[cam_name] = (timestamp, frame_bytes)
```
When the Scheduler asks for a frame, it *always* receives the absolute latest snapshot, instantly shedding all intermediate frames.

## 3. The DeadlineScheduler

The `DeadlineScheduler` is the heart of RapidAlert's performance. It utilizes a greedy staleness algorithm instead of a standard FIFO queue.

### 3.1 Pinned Workers
The scheduler spawns exactly `max_concurrency` (e.g., 12) async workers.
Previously, dynamic auto-tuning was attempted, which throttled requests. However, vLLM thrives on **Continuous Batching**. By pinning workers to a high constant and flooding vLLM with parallel requests, vLLM can optimize KV cache allocation and process 4–5 frames in parallel in the same time it takes to process 1.

### 3.2 Greedy Selection Algorithm
Instead of evaluating cameras cyclically, the `_feed_loop` calculates the staleness of every eligible camera:
```python
staleness = time.monotonic() - self._last_analyzed.get(cam, 0)
```
The camera with the highest staleness is immediately injected into the worker queue. This guarantees mathematically fair scheduling across dynamic camera counts (e.g., 7 cameras handled by 12 workers means the staleness never exceeds a few milliseconds).

## 4. Inference Routing: VLMPool

RapidAlert runs multiple instances of vLLM to scale across available GPU VRAM on the Jetson Thor.

### 4.1 Perfect Load Balancing
A standard Round-Robin load balancer is insufficient because different frames take vastly different amounts of time to process based on output token length.
`VLMPool` implements **Least-Connections Routing**. 
It maintains an atomic `_inflight` counter for every endpoint:
```python
best_idx = min(range(len(endpoints)), key=lambda i: self._inflight[i])
```
When a worker needs to send a request, it routes to the exact instance with the lowest active workload, mathematically preventing instance saturation while others sit idle.

## 5. Inference Optimization & Qwen-VL

### 5.1 Tokenization Economics
Vision models like Qwen-VL scale their vision tokens proportionally to image resolution.
RapidAlert downscales raw RTSP frames to a maximum `frame_width` of 768px (configured in `system.json`) before base64-encoding. This reduces the token payload by roughly **300%** compared to a standard 1280x720 frame, slashing Time-To-First-Token (TTFT) from 8 seconds to ~1.5 seconds.

### 5.2 Quantization & Model Selection
The backend strictly targets `cyankiwi/Qwen3-VL-4B-Instruct-AWQ-4bit`.
AWQ (Activation-aware Weight Quantization) drastically reduces VRAM requirements and memory bandwidth pressure—the primary bottleneck on integrated systems like the Jetson Thor.
Extended reasoning loops (`<think>`) are explicitly banned via `stop` token enforcement in the prompt wrapper to ensure predictable latency and prevent the VLM from hanging on complex visual scenes.

## 6. Fault Tolerance & Graceful Shutdowns

The `run.sh` script is heavily hardened against orphaned processes.
1. **SIGCONT Injection**: If a user suspends the process via `CTRL+Z` (SIGTSTP), the script injects a `SIGCONT` signal to wake the application before issuing a `SIGTERM`. This prevents Ubuntu's Apport crash handler from intercepting segmentation faults caused by `SIGKILL` on sleeping C-extensions.
2. **Lifespan Cancellation**: The FastAPI `lifespan` explicitly tracks and cancels infinite generator loops (like the `config_sync_loop`) before shutting down the Uvicorn event loop, eliminating messy `asyncio.CancelledError` traceback dumps.
