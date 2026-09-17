# 🚨 RapidAlert: Ultra-Fast VLM CCTV Engine

RapidAlert is a high-performance, real-time Vision-Language Model (VLM) surveillance engine designed specifically for the NVIDIA Jetson Thor platform. It processes multi-camera RTSP feeds simultaneously, utilizing Qwen3-VL to provide zero-latency natural language analysis of physical spaces.

## ✨ Key Features

* **Massive Concurrency:** Capable of processing 7+ camera feeds simultaneously with a 12-worker async Python scheduler leveraging vLLM's continuous batching.
* **Intelligent Routing:** Custom `VLMPool` architecture load-balances incoming frames across multiple localized vLLM Docker containers using real-time in-flight request tracking.
* **Zero-Latency Ingestion:** Dedicated `CameraManager` threads guarantee that only the absolute freshest frame is ever sent to the VLM, dropping stale frames instantly.
* **Jetson Thor Optimized:** Exploits the Jetson Thor's massive unified memory pool, running three 4-bit AWQ quantized Qwen-VL instances in parallel.
* **Dynamic Prompts:** Master and camera-specific overriding prompts allow fine-tuning the AI's attention (e.g., watching for PPE in a factory vs. spills in a cafeteria).
* **Real-Time Dashboard:** A responsive, glassmorphism-styled frontend built with vanilla HTML/JS/CSS and WebSockets.

## 🚀 Getting Started

### Prerequisites
* NVIDIA Jetson Thor (or similar high-VRAM unified memory Linux environment).
* Docker with NVIDIA Runtime installed.
* Python 3.12+

### Installation
```bash
# Clone the repository
git clone https://github.com/kaushikvarma-create/RapidAlert.git
cd RapidAlert

# Install python dependencies (handled automatically by run.sh if missing)
pip install -r requirements.txt
```

### Running the Engine
```bash
./run.sh
```

The startup script will:
1. Ensure all orphaned/zombie background processes are cleanly terminated.
2. Spin up the configured number of vLLM containers via Docker.
3. Wait for the endpoints to become healthy.
4. Launch the FastAPI backend and WebSocket server.
5. Open the dashboard at `http://localhost:7000`.

## 📁 Repository Structure

* `run.sh`: The master orchestrator script for Docker and the FastAPI app.
* `backend/`: Python core logic (Scheduler, Frame Ingestion, VLM Client, Alert Engine).
* `frontend/`: Real-time dashboard UI (HTML, CSS, JS).
* `config/`: System configuration (`system.json`), camera setups (`cameras.json`), and prompts (`prompts.json`).
* `data/`: SQLite databases for persistent event logs.
* `docs/`: Extensive technical deep-dive and architectural documentation.

## 🛠 Configuration

Modify `config/system.json` to tune system limits:
* `vllm_instances`: Number of parallel vLLM nodes to spawn (default: 3).
* `vllm_model`: The HuggingFace model string (e.g., `cyankiwi/Qwen3-VL-4B-Instruct-AWQ-4bit`).
* `frame_width`: Internal resolution to scale frames to before sending to the VLM (default: 768px to drastically reduce token count).
* `max_concurrency`: Total number of active Python async workers feeding the vLLM nodes (default: 12).

## 📖 Further Reading

For a comprehensive breakdown of the greedy scheduling algorithm, VLM continuous batching, and system architecture, please see the [Technical Deep Dive](docs/technical_deep_dive.md).
