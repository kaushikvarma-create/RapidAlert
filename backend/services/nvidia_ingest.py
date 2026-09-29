"""
NvidiaStreamCapture: Hardware-accelerated RTSP video ingest for NVIDIA Thor / Jetson.
Uses GStreamer and DeepStream (nvurisrcbin + nvvideoconvert) to decode H.264/H.265
directly on NVDEC, bypassing CPU decoding overhead.
"""
from __future__ import annotations

import os
import time
import subprocess
from typing import Optional, Tuple
import numpy as np
import cv2

from backend.core.config import DEFAULT_FRAME_WIDTH, DEFAULT_FRAME_HEIGHT
from backend.core.error_tracker import error_tracker

_GST_INITIALIZED = False
_NVIDIA_AVAILABLE: Optional[bool] = None


def _ensure_mig_device_configured() -> None:
    """If MIG is enabled on Thor/Hopper, ensure CUDA_VISIBLE_DEVICES is set to a valid MIG UUID."""
    if not os.environ.get("CUDA_VISIBLE_DEVICES"):
        try:
            out = subprocess.check_output(["nvidia-smi", "-L"], text=True, timeout=2)
            for line in out.strip().splitlines():
                if "MIG" in line and "UUID: MIG-" in line:
                    mig_uuid = line.split("UUID: ")[1].strip().rstrip(")")
                    os.environ["CUDA_VISIBLE_DEVICES"] = mig_uuid
                    print(f"[NvidiaIngest] 🎯 Auto-configured CUDA_VISIBLE_DEVICES={mig_uuid} for MIG NVDEC decoding")
                    break
        except Exception:
            pass


def is_nvidia_available() -> bool:
    """Check if GStreamer and nvurisrcbin / nvvideoconvert plugins are present."""
    global _NVIDIA_AVAILABLE, _GST_INITIALIZED
    if _NVIDIA_AVAILABLE is not None:
        return _NVIDIA_AVAILABLE
    try:
        _ensure_mig_device_configured()
        import gi
        gi.require_version("Gst", "1.0")
        gi.require_version("GstApp", "1.0")
        from gi.repository import Gst
        if not _GST_INITIALIZED:
            Gst.init(None)
            _GST_INITIALIZED = True
        
        has_src = Gst.ElementFactory.find("nvurisrcbin") is not None
        has_conv = Gst.ElementFactory.find("nvvideoconvert") is not None
        _NVIDIA_AVAILABLE = bool(has_src and has_conv)
    except Exception as exc:
        error_tracker.capture_exception(
            exc,
            component="NvidiaIngest",
            effect="GStreamer / DeepStream plugins probe failed; NVDEC hardware decoding unavailable",
            severity="WARNING",
        )
        _NVIDIA_AVAILABLE = False
    return _NVIDIA_AVAILABLE


def is_deepstream_available() -> bool:
    """Check if DeepStream batched elements (nvurisrcbin, nvstreammux, nvvideoconvert) are present."""
    if not is_nvidia_available():
        return False
    try:
        from gi.repository import Gst
        return Gst.ElementFactory.find("nvstreammux") is not None
    except Exception:
        return False



class NvidiaStreamCapture:
    """
    Drop-in replacement for cv2.VideoCapture using GStreamer + DeepStream hardware decoding.
    """
    def __init__(self, uri: str, width: int = DEFAULT_FRAME_WIDTH, height: int = DEFAULT_FRAME_HEIGHT):
        self.uri = uri
        self.width = width
        self.height = height
        self._pipeline = None
        self._sink = None
        self._bus = None
        self._opened = False

        if not is_nvidia_available():
            raise RuntimeError("NVIDIA GStreamer / DeepStream plugins not available")

        self._start_pipeline()

    def _start_pipeline(self) -> None:
        _ensure_mig_device_configured()
        import gi
        gi.require_version("Gst", "1.0")
        gi.require_version("GstApp", "1.0")
        from gi.repository import Gst

        pipeline_str = (
            f'nvurisrcbin uri="{self.uri}" select-rtp-protocol=4 rtsp-reconnect-interval=5 latency=200 drop-frame-interval=0 ! '
            f'nvvideoconvert ! '
            f'video/x-raw, width={self.width}, height={self.height}, format=RGBA ! '
            f'appsink name=sink emit-signals=true max-buffers=2 drop=true sync=false'
        )

        try:
            self._pipeline = Gst.parse_launch(pipeline_str)
            self._sink = self._pipeline.get_by_name("sink")
            self._bus = self._pipeline.get_bus()
            
            # Start pipeline
            ret = self._pipeline.set_state(Gst.State.PLAYING)
            from gi.repository import Gst as GstEnum
            if ret == GstEnum.StateChangeReturn.FAILURE:
                self.release()
                self._opened = False
                error_tracker.capture_error(
                    message=f"Pipeline state change to PLAYING failed for {self.uri}",
                    component="NvidiaIngest",
                    effect="Failed to start DeepStream NVDEC pipeline; falling back to CPU decoder",
                    severity="WARNING",
                )
            else:
                self._opened = True
        except Exception as e:
            error_tracker.capture_exception(
                e,
                component="NvidiaIngest",
                effect=f"Failed to build DeepStream pipeline for {self.uri}; falling back to CPU",
                severity="WARNING",
            )
            self.release()
            self._opened = False

    def isOpened(self) -> bool:
        return self._opened and self._pipeline is not None

    def read(self, timeout_sec: float = 1.0) -> Tuple[bool, Optional[np.ndarray]]:
        if not self.isOpened():
            return False, None

        try:
            from gi.repository import Gst

            # Check bus for errors or EOS
            if self._bus:
                msg = self._bus.pop_filtered(Gst.MessageType.ERROR | Gst.MessageType.EOS)
                if msg:
                    if msg.type == Gst.MessageType.ERROR:
                        err, debug = msg.parse_error()
                        error_tracker.capture_error(
                            message=f"Bus error: {err}, debug: {debug}",
                            component="NvidiaIngest",
                            effect=f"DeepStream bus error on {self.uri}; closing pipeline",
                            severity="ERROR",
                        )
                    self._opened = False
                    return False, None

            timeout_ns = int(timeout_sec * 1_000_000_000)
            sample = self._sink.emit("try-pull-sample", timeout_ns)
            if sample is None:
                return False, None

            buf = sample.get_buffer()
            if buf is None:
                return False, None

            caps = sample.get_caps()
            if caps is None:
                return False, None
            s = caps.get_structure(0)
            w, h = s.get_value("width"), s.get_value("height")

            success, map_info = buf.map(Gst.MapFlags.READ)
            if not success:
                return False, None

            try:
                # Memory view to numpy RGBA then BGR
                arr = np.frombuffer(map_info.data, dtype=np.uint8).reshape((h, w, 4))
                bgr = cv2.cvtColor(arr, cv2.COLOR_RGBA2BGR)
                # Validate frame sanity: drop uninitialized YUV420 buffers (which map to solid green B<25, R<25, G>90)
                mean_b, mean_g, mean_r = cv2.mean(bgr)[:3]
                if mean_g > 90 and mean_r < 25 and mean_b < 25:
                    return False, None
                if mean_g < 3 and mean_r < 3 and mean_b < 3:
                    return False, None
                return True, bgr
            finally:
                try:
                    buf.unmap(map_info)
                except Exception:
                    pass
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="NvidiaIngest",
                effect=f"Exception reading NVDEC frame for {self.uri}",
                severity="WARNING",
            )
            return False, None

    def release(self) -> None:
        self._opened = False
        if self._pipeline:
            try:
                from gi.repository import Gst
                self._pipeline.set_state(Gst.State.NULL)
            except Exception:
                pass
            self._pipeline = None
            self._sink = None
            self._bus = None


class DeepStreamBatchedCapture:
    """
    Batched multi-camera hardware-accelerated video ingestion using nvstreammux + NVDEC.
    Decodes multiple RTSP streams in parallel on NVDEC, multiplexes into batched NVMM buffers,
    and dispatches extracted frames directly into the FrameStore.
    """
    def __init__(
        self,
        frame_store,
        width: int = DEFAULT_FRAME_WIDTH,
        height: int = DEFAULT_FRAME_HEIGHT,
        batch_timeout_us: int = 33000,
        compute_hw: int = 1,
    ):
        self.frame_store = frame_store
        self.width = width
        self.height = height
        self.batch_timeout_us = batch_timeout_us
        self.compute_hw = compute_hw

        self._cameras: dict[str, str] = {}
        self._pipeline = None
        self._mux = None
        self._sink = None
        self._bus = None
        self._running = False

    @staticmethod
    def is_available() -> bool:
        return is_deepstream_available()

