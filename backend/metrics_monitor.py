import asyncio
import subprocess
import time

async def get_gpu_util():
    try:
        proc = await asyncio.create_subprocess_shell(
            "nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        return int(stdout.decode().strip())
    except Exception:
        return 0

def get_cpu_util(prev_idle, prev_total):
    try:
        with open('/proc/stat', 'r') as f:
            lines = f.readlines()
        cpu_line = lines[0].split()
        idle = float(cpu_line[4]) + float(cpu_line[5])
        total = sum(float(x) for x in cpu_line[1:])
        
        diff_idle = idle - prev_idle
        diff_total = total - prev_total
        
        util = 100 * (1.0 - diff_idle / diff_total) if diff_total > 0 else 0
        return round(util, 1), idle, total
    except Exception:
        return 0, prev_idle, prev_total

def get_ram_util():
    try:
        with open('/proc/meminfo', 'r') as f:
            lines = f.readlines()
        mem_total = 0
        mem_avail = 0
        for line in lines:
            if line.startswith('MemTotal:'):
                mem_total = int(line.split()[1])
            elif line.startswith('MemAvailable:'):
                mem_avail = int(line.split()[1])
        if mem_total > 0:
            return round(100 * (1.0 - (mem_avail / mem_total)), 1)
        return 0
    except Exception:
        return 0

async def metrics_loop(broadcast_fn):
    prev_idle, prev_total = 0, 0
    _, prev_idle, prev_total = get_cpu_util(prev_idle, prev_total)
    
    while True:
        await asyncio.sleep(2)
        gpu = await get_gpu_util()
        cpu, prev_idle, prev_total = get_cpu_util(prev_idle, prev_total)
        ram = get_ram_util()
        
        if broadcast_fn:
            await broadcast_fn({
                "type": "sys_metrics",
                "gpu": gpu,
                "cpu": cpu,
                "ram": ram
            })
