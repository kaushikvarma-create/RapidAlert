"""
NvidiaStreamCapture: Hardware-accelerated RTSP video ingest for NVIDIA Thor / Jetson.
Uses GStreamer and DeepStream (nvurisrcbin + nvvideoconvert) to decode H.264/H.265
directly on NVDEC, bypassing CPU decoding overhead.
"""
import time
from typing import Optional, Tuple
import numpy as np
import cv2

_GST_INITIALIZED = False
_NVIDIA_AVAILABLE: Optional[bool] = None

def is_nvidia_available() -> bool:
    """Check if GStreamer and nvurisrcbin / nvvideoconvert plugins are present."""
    global _NVIDIA_AVAILABLE, _GST_INITIALIZED
    if _NVIDIA_AVAILABLE is not None:
        return _NVIDIA_AVAILABLE
    try:
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
    except Exception:
        _NVIDIA_AVAILABLE = False
    return _NVIDIA_AVAILABLE


class NvidiaStreamCapture:
    """
    Drop-in replacement for cv2.VideoCapture using GStreamer + DeepStream hardware decoding.
    """
    def __init__(self, uri: str, width: int = 1280, height: int = 720):
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
        import gi
        gi.require_version("Gst", "1.0")
        gi.require_version("GstApp", "1.0")
        from gi.repository import Gst

        pipeline_str = (
            f'nvurisrcbin uri="{self.uri}" ! '
            f'nvvideoconvert ! '
            f'video/x-raw, width={self.width}, height={self.height}, format=RGBA ! '
            f'appsink name=sink emit-signals=true max-buffers=1 drop=true'
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
            else:
                self._opened = True
        except Exception as e:
            print(f"[NvidiaIngest] Failed to build pipeline for {self.uri}: {e}")
            self.release()
            self._opened = False

    def isOpened(self) -> bool:
        return self._opened and self._pipeline is not None

    def read(self, timeout_sec: float = 1.0) -> Tuple[bool, Optional[np.ndarray]]:
        if not self.isOpened():
            return False, None

        from gi.repository import Gst

        # Check bus for errors or EOS
        if self._bus:
            msg = self._bus.pop_filtered(Gst.MessageType.ERROR | Gst.MessageType.EOS)
            if msg:
                if msg.type == Gst.MessageType.ERROR:
                    err, debug = msg.parse_error()
                    print(f"[NvidiaIngest] Bus error: {err}, debug: {debug}")
                self._opened = False
                return False, None

        timeout_ns = int(timeout_sec * 1_000_000_000)
        sample = self._sink.emit("try-pull-sample", timeout_ns)
        if sample is None:
            return False, None

        buf = sample.get_buffer()
        caps = sample.get_caps()
        s = caps.get_structure(0)
        w, h = s.get_value("width"), s.get_value("height")

        success, map_info = buf.map(Gst.MapFlags.READ)
        if not success:
            return False, None

        try:
            # Memory view to numpy RGBA then BGR
            arr = np.frombuffer(map_info.data, dtype=np.uint8).reshape((h, w, 4))
            bgr = cv2.cvtColor(arr, cv2.COLOR_RGBA2BGR)
            return True, bgr
        except Exception as e:
            print(f"[NvidiaIngest] Buffer parse error: {e}")
            return False, None
        finally:
            buf.unmap(map_info)

    def release(self) -> None:
        self._opened = False
        if self._pipeline:
            from gi.repository import Gst
            self._pipeline.set_state(Gst.State.NULL)
            self._pipeline = None
            self._sink = None
            self._bus = None
