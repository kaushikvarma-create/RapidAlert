#!/usr/bin/env python3
"""
Performance & Load Benchmark:
Measures CPU, RAM, MediaMTX process stats, stream proxy throughput, and response latency.
"""
import time
import json
import urllib.request
import psutil

def run_benchmark():
    print("=" * 60)
    print("      RapidAlert Streaming & System Load Benchmark")
    print("=" * 60)

    # 1. System-wide metrics
    cpu_pct = psutil.cpu_percent(interval=1.0)
    ram = psutil.virtual_memory()
    print(f"System CPU Usage: {cpu_pct:.1f}%")
    print(f"System RAM: {ram.used / (1024**3):.2f} GB / {ram.total / (1024**3):.2f} GB ({ram.percent:.1f}%)")

    # 2. MediaMTX process metrics
    mtx_procs = [p for p in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_info']) 
                 if 'mediamtx' in (p.info['name'] or '') or any('mediamtx' in arg for arg in (p.info['cmdline'] or []))]
    
    print("\n--- MediaMTX Daemon Stats ---")
    if mtx_procs:
        for p in mtx_procs:
            p_cpu = p.cpu_percent(interval=0.2)
            p_mem = p.memory_info().rss / (1024 * 1024)
            print(f"PID: {p.pid} | Process: {p.name()} | CPU: {p_cpu:.1f}% | RAM: {p_mem:.1f} MB")
    else:
        print("MediaMTX process not detected in psutil filter.")

    # 3. Stream Proxy Latency & Throughput Benchmark
    print("\n--- Stream Endpoint Benchmark (LL-HLS via FastAPI Reverse Proxy) ---")
    cams = ["server_entry", "1st_entrance", "parking", "1st_out", "admin_cabin", "1st_exit", "reception"]
    
    latencies = []
    for cam in cams:
        t0 = time.perf_counter()
        url = f"http://127.0.0.1:7000/stream/{cam}/index.m3u8"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                data = resp.read()
                dur_ms = (time.perf_counter() - t0) * 1000
                latencies.append(dur_ms)
                has_hvc1 = b"hvc1" in data or b"hev1" in data
                has_avc1 = b"avc1" in data
                codec = "H.265 (HEVC)" if has_hvc1 else ("H.264 (AVC)" if has_avc1 else "Auto/fMP4")
                print(f"  ✓ {cam:15s} | HTTP {resp.status} | Latency: {dur_ms:5.1f}ms | Manifest: {len(data)} B | Codec: {codec}")
        except Exception as e:
            print(f"  ⚠ {cam:15s} | Request failed: {e}")

    if latencies:
        avg_lat = sum(latencies) / len(latencies)
        print(f"\nAverage Manifest Latency: {avg_lat:.2f} ms")

    # 4. FastAPI Telemetry
    print("\n--- FastAPI Engine Telemetry ---")
    try:
        with urllib.request.urlopen("http://127.0.0.1:7000/api/config", timeout=2.0) as resp:
            cfg = json.loads(resp.read().decode())
            print(f"Configured Cameras: {len(cfg.get('cameras', []))}")
    except Exception as e:
        print(f"Telemetry query error: {e}")

    print("=" * 60)

if __name__ == "__main__":
    run_benchmark()
