"""
ClipRecorder: Asynchronously records and encodes incident video clips (MP4).
Stitches temporal multi-frame sequences into web-optimized H.264 video clips
with rolling buffer pruning to maintain storage limits.
"""
from __future__ import annotations

import asyncio
import base64
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional, Union

import cv2
import numpy as np

from backend.core.config import CLIPS_DIR
from backend.core.error_tracker import error_tracker


class ClipRecorder:
    def __init__(
        self,
        clips_dir: Path = CLIPS_DIR,
        max_clips: int = 200,
        max_total_mb: int = 1024,
        default_fps: float = 2.0,
    ):
        self.clips_dir = Path(clips_dir)
        self.clips_dir.mkdir(parents=True, exist_ok=True)
        self.max_clips = max_clips
        self.max_total_mb = max_total_mb
        self.default_fps = default_fps
        self._lock = threading.Lock()
        self._has_ffmpeg = shutil.which("ffmpeg") is not None

    def create_clip(
        self,
        cam_name: str,
        event_id: str,
        frames: Union[List[np.ndarray], List[str]],
        fps: Optional[float] = None,
    ) -> Optional[str]:
        """
        Synchronously (or inside thread worker) stitches frames into an MP4 clip.
        Returns the web-accessible relative URL (e.g. '/clips/<event_id>.mp4') or None.
        """
        if not frames or len(frames) < 2:
            return None

        clean_event_id = "".join(c for c in event_id if c.isalnum() or c in ("-", "_"))
        if not clean_event_id:
            clean_event_id = f"clip_{int(time.time() * 1000)}"

        target_file = self.clips_dir / f"{clean_event_id}.mp4"
        fps_val = fps or self.default_fps

        try:
            # 1. Decode / prepare numpy frames
            np_frames: List[np.ndarray] = []
            for item in frames:
                if isinstance(item, np.ndarray):
                    np_frames.append(item)
                elif isinstance(item, str) and item:
                    # Base64 JPEG string
                    try:
                        b64_str = item.split(",")[-1] if "," in item else item
                        img_bytes = base64.b64decode(b64_str)
                        nparr = np.frombuffer(img_bytes, np.uint8)
                        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                        if img is not None:
                            np_frames.append(img)
                    except Exception:
                        continue

            if len(np_frames) < 2:
                return None

            # Standardize resolution (match first frame)
            h, w = np_frames[0].shape[:2]
            # Ensure dimensions are even numbers for H.264 encoder compatibility
            if w % 2 != 0:
                w -= 1
            if h % 2 != 0:
                h -= 1

            resized_frames = []
            for f in np_frames:
                if f.shape[0] != h or f.shape[1] != w:
                    resized_frames.append(cv2.resize(f, (w, h), interpolation=cv2.INTER_AREA))
                else:
                    resized_frames.append(f)

            # 2. Render initial video via OpenCV
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                tmp_path = tmp.name

            fourcc = cv2.VideoWriter_fourcc(*"mp4v")
            writer = cv2.VideoWriter(tmp_path, fourcc, fps_val, (w, h))
            for f in resized_frames:
                # Repeat each frame 2 times to make short incident snippets easily watchable
                for _ in range(2):
                    writer.write(f)
            writer.release()

            # 3. Transcode to web-optimized H.264 (faststart) if ffmpeg available
            if self._has_ffmpeg:
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-i",
                    tmp_path,
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                    "-preset",
                    "ultrafast",
                    "-movflags",
                    "+faststart",
                    str(target_file),
                ]
                proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                if proc.returncode == 0 and target_file.exists():
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                else:
                    # Fallback to tmp_path copy
                    shutil.move(tmp_path, str(target_file))
            else:
                shutil.move(tmp_path, str(target_file))

            # 4. Prune old clips asynchronously
            self.prune()

            return f"/clips/{clean_event_id}.mp4"

        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="ClipRecorder",
                camera=cam_name,
                effect=f"Failed to render incident video clip for {event_id}",
                severity="WARNING",
            )
            return None

    async def async_create_clip(
        self,
        cam_name: str,
        event_id: str,
        frames: Union[List[np.ndarray], List[str]],
        fps: Optional[float] = None,
    ) -> Optional[str]:
        """Runs create_clip in background thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self.create_clip,
            cam_name,
            event_id,
            frames,
            fps,
        )

    def prune(self) -> None:
        """Enforces max_clips and max_total_mb retention policy."""
        try:
            with self._lock:
                files = list(self.clips_dir.glob("*.mp4"))
                if not files:
                    return

                # Sort by modification time ascending (oldest first)
                files.sort(key=lambda p: p.stat().st_mtime)

                # Total size check
                total_bytes = sum(p.stat().st_size for p in files)
                max_bytes = self.max_total_mb * 1024 * 1024

                while files and (len(files) > self.max_clips or total_bytes > max_bytes):
                    oldest = files.pop(0)
                    try:
                        sz = oldest.stat().st_size
                        oldest.unlink()
                        total_bytes -= sz
                    except OSError:
                        pass
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="ClipRecorder",
                effect="Failed to prune old incident video clips",
                severity="WARNING",
            )


# Global singleton instance
clip_recorder = ClipRecorder()
