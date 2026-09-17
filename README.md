# 🚀 RapidAlert - Model Testing Tensorboard

Welcome to the **Model Testing** branch of RapidAlert. 
This branch completely forks the original RapidAlert unified-CCTV architecture into a **highly optimized, concurrent A/B/C testing environment** designed specifically for benchmarking Vision-Language Models (VLMs) on the NVIDIA Jetson Thor platform.

---

## 🏗️ Architectural Overview

Unlike the main branch, which uses a Least-Connections router to load-balance CCTV feeds across instances of the same model, this branch operates as a synchronous **Comparator Engine**. 

### 1. Per-Instance Model Spawning (`run.sh`)
When booting up, `run.sh` no longer limits the VLM pool to a single global model. It deeply integrates with `config/system.json`:
- It parses the `vllm_endpoints` JSON array.
- For `VLLM_INSTANCES=3`, it assigns endpoint 0 to `model_0`, endpoint 1 to `model_1`, etc.
- This allows 3 distinct AWQ-quantized VLMs (e.g. Qwen3-VL-4B, LLaVA-1.5, InternVL) to reside in memory across the unified Jetson Thor RAM footprint.

### 2. Concurrent Execution Engine (`backend/scheduler.py` & `backend/vlm_client.py`)
The `DeadlineScheduler` was entirely re-engineered. 
- **Trigger**: Driven by user selection from the Frontend WebSocket stream (`selected_cameras`).
- **Dispatch**: The scheduler fetches the latest base64 snapshot from the active camera.
- **Gather (`asyncio.gather`)**: It triggers `analyze_concurrent()` in the `VLMPool`, firing the EXACT same frame and prompt to all 3 endpoints concurrently.
- **Consolidation**: Instead of returning a single `dict`, it returns `list[dict]` containing the side-by-side responses and distinct time-to-first-token (TTFT)/latency metrics. 

### 3. Native Hardware Monitoring (`backend/metrics_monitor.py`)
To prevent crashes on Jetson due to heavy LLM footprint, hardware utilization is monitored at 2Hz using native hooks, bypassing the need for bulky PIP libraries:
- **GPU Usage**: Polled asynchronously using `nvidia-smi --query-gpu=utilization.gpu`.
- **CPU & Unified RAM Usage**: Sourced natively via `/proc/stat` and `/proc/meminfo` differential parsing.
These metrics are packaged into a `sys_metrics` WS event and flushed to the dashboard.

---

## 🖥️ Frontend Tensorboard Redesign

The Vanilla HTML/JS frontend has been rebuilt from the ground up for analytical clarity.

1. **Top HUD**: 
   Displays real-time hardware telemetry (GPU, CPU, RAM) rather than production analytics.
2. **Camera Target Selection**: 
   A left sidebar dynamically lists all detected IP streams. Checking a stream assigns it to the Tensorboard active queue.
3. **Multi-Model Main Stage**: 
   Each selected stream generates a dedicated wide-row on the main stage. 
   - **Left side**: Real-time thumbnail (double-buffered in JS to prevent DOM flicker).
   - **Right side**: A dynamically generated 3-column grid, showcasing the live inference observation, severity tagging, and generation latency side-by-side for each of the 3 VLMs.

---

## ⚙️ Configuration & Execution

1. Edit `config/system.json`:
   ```json
   "vllm_endpoints": [
     {"url": "http://localhost:8000", "model": "cyankiwi/Qwen3.5-4B-AWQ-4bit"},
     {"url": "http://localhost:8001", "model": "other/Model-A"},
     {"url": "http://localhost:8002", "model": "other/Model-B"}
   ]
   ```
2. **Run System**:
   ```bash
   ./run.sh
   ```
3. Open `http://localhost:7000` to access the Tensorboard!
