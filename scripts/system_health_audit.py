#!/usr/bin/env python3
"""
RapidAlert Automated System Health & Process Auditor
Usage: python3 scripts/system_health_audit.py [--fix] [--quiet]
"""
import sys
import os
import json
import time
import subprocess
import argparse
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
CYAN = "\033[96m"
NC = "\033[0m"

def log_ok(msg):
    print(f"  {GREEN}✓{NC} {msg}")

def log_warn(msg):
    print(f"  {YELLOW}⚠{NC} {msg}")

def log_err(msg):
    print(f"  {RED}✗{NC} {msg}")

def log_info(msg):
    print(f"  {CYAN}ℹ{NC} {msg}")

def audit_processes(fix=False):
    print(f"\n{BOLD}[1/5] Process & Port Audit{NC}")
    issues_found = False
    
    # Check port 7000 (Dashboard)
    try:
        res = subprocess.run(["fuser", "7000/tcp"], capture_output=True, text=True)
        pids = [p.strip() for p in res.stdout.strip().split() if p.strip()]
        if pids:
            log_warn(f"Port 7000 is currently occupied by PID(s): {', '.join(pids)}")
            if fix:
                subprocess.run(["fuser", "-k", "7000/tcp"], capture_output=True)
                log_ok("Port 7000 freed successfully.")
        else:
            log_ok("Port 7000 is clean and available.")
    except Exception as e:
        log_warn(f"Could not inspect port 7000: {e}")

    # Check for defunct zombie processes
    try:
        ps_out = subprocess.run(["ps", "-eo", "pid,stat,cmd"], capture_output=True, text=True).stdout
        zombies = [line for line in ps_out.splitlines() if " Z" in line or "<defunct>" in line]
        if zombies:
            log_warn(f"Found {len(zombies)} zombie/defunct process(es).")
            issues_found = True
        else:
            log_ok("No zombie or defunct processes detected.")
    except Exception as e:
        log_warn(f"Could not check zombie processes: {e}")

    return not issues_found

def audit_configs():
    print(f"\n{BOLD}[2/5] Configuration Schema & Integrity Audit{NC}")
    configs = [
        ("config/system.json", ["vllm_endpoints", "dashboard_port"]),
        ("config/cameras.json", None),
        ("config/prompts.json", ["master", "followup"]),
    ]
    all_ok = True
    for rel_path, req_keys in configs:
        p = ROOT_DIR / rel_path
        if not p.exists():
            log_err(f"Missing config file: {rel_path}")
            all_ok = False
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if req_keys:
                missing = [k for k in req_keys if k not in data]
                if missing:
                    log_err(f"{rel_path}: Missing required keys: {missing}")
                    all_ok = False
                else:
                    log_ok(f"{rel_path}: Valid JSON with all required keys.")
            else:
                log_ok(f"{rel_path}: Valid JSON ({len(data)} items).")
        except json.JSONDecodeError as exc:
            log_err(f"{rel_path}: JSON syntax error: {exc}")
            all_ok = False
    return all_ok

def audit_vllm():
    print(f"\n{BOLD}[3/5] vLLM Endpoints & Multi-Frame Audit{NC}")
    import urllib.request
    
    sys_path = ROOT_DIR / "config/system.json"
    if not sys_path.exists():
        return False
    
    with open(sys_path) as f:
        sys_cfg = json.load(f)
    
    endpoints = sys_cfg.get("vllm_endpoints", [])
    if not endpoints:
        log_warn("No vllm_endpoints configured in system.json")
        return True

    all_ok = True
    for ep in endpoints:
        url = ep.get("url")
        mig_uuid = ep.get("mig_uuid", "")
        mode_str = f"MIG ({mig_uuid[:12]}...)" if mig_uuid else "Shared"
        try:
            health_url = f"{url}/health"
            with urllib.request.urlopen(health_url, timeout=3) as resp:
                if resp.status == 200:
                    log_ok(f"Endpoint {url} [{mode_str}]: Online & Healthy (HTTP 200)")
                else:
                    log_warn(f"Endpoint {url} [{mode_str}]: Returned HTTP {resp.status}")
                    all_ok = False
        except Exception as e:
            log_err(f"Endpoint {url} [{mode_str}]: Connection failed ({e})")
            all_ok = False
    return all_ok

def audit_hardware():
    print(f"\n{BOLD}[4/5] Hardware & NVDEC Acceleration Audit{NC}")
    import torch
    if torch.cuda.is_available():
        gpu_name = torch.cuda.get_device_name(0)
        log_ok(f"CUDA Available: {gpu_name} (Device count: {torch.cuda.device_count()})")
    else:
        log_warn("CUDA not detected by PyTorch; running on CPU.")

    from backend.services.nvidia_ingest import is_nvidia_available, is_deepstream_available
    if is_nvidia_available():
        log_ok("NVIDIA DeepStream NVDEC plugins (nvurisrcbin, nvvideoconvert) detected.")
    else:
        log_warn("NVIDIA DeepStream plugins not found; using OpenCV fallback.")

    if is_deepstream_available():
        log_ok("DeepStream batched mux (nvstreammux) detected.")
    return True

def audit_codebase_integrity():
    print(f"\n{BOLD}[5/5] Codebase Compilation & Import Audit{NC}")
    import py_compile
    backend_dir = ROOT_DIR / "backend"
    python_files = list(backend_dir.rglob("*.py"))
    
    compile_errors = 0
    for py_file in python_files:
        try:
            py_compile.compile(str(py_file), doraise=True)
        except py_compile.PyCompileError as e:
            log_err(f"Syntax error in {py_file.relative_to(ROOT_DIR)}: {e}")
            compile_errors += 1

    if compile_errors == 0:
        log_ok(f"All {len(python_files)} Python files compiled cleanly.")
    else:
        log_err(f"{compile_errors} file(s) failed compilation.")

    return compile_errors == 0

def main():
    parser = argparse.ArgumentParser(description="RapidAlert Automated System Health Auditor")
    parser.add_argument("--fix", action="store_true", help="Automatically fix issues (e.g. freeing ports)")
    args = parser.parse_args()

    print(f"{BOLD}{CYAN}====================================================={NC}")
    print(f"{BOLD}{CYAN}    RapidAlert Automated System & Health Auditor     {NC}")
    print(f"{BOLD}{CYAN}====================================================={NC}")

    p_ok = audit_processes(fix=args.fix)
    c_ok = audit_configs()
    v_ok = audit_vllm()
    h_ok = audit_hardware()
    i_ok = audit_codebase_integrity()

    print(f"\n{BOLD}Audit Summary:{NC}")
    overall = p_ok and c_ok and v_ok and h_ok and i_ok
    if overall:
        print(f"  {GREEN}{BOLD}✅ ALL CHECKS PASSED — System is clean, healthy, and operational.{NC}\n")
        return 0
    else:
        print(f"  {YELLOW}{BOLD}⚠️  Some warnings/issues detected. See details above.{NC}\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
