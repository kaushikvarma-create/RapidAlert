"""
SceneTriggerEngine: DINOv2-based lightweight real-time scene change detector.
Runs facebook/dinov2-small on CUDA at 1-2 FPS per active camera.
Computes cosine embedding drift between consecutive samples.
When drift exceeds the threshold, captures pre-trigger context, awaits post-trigger progression,
and dispatches an Incident Event to the Scheduler for Cosmos Reason2 8B evaluation.
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable, Dict, List, Optional
import cv2
import numpy as np
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModel

from backend.core.config import (
    DEFAULT_DINOV2_MODEL,
    DEFAULT_SCENE_THRESHOLD,
    DEFAULT_SEMANTIC_INTERVAL,
    DEFAULT_EVENT_COOLDOWN,
    DINOV2_INPUT_WIDTH,
    HIGH_RES_FRAME_WIDTH,
    HIGH_RES_JPEG_QUALITY,
    TEMPORAL_THUMB_WIDTH,
    TEMPORAL_THUMB_QUALITY,
)
from backend.core.error_tracker import error_tracker


class SceneTriggerEngine:
    def __init__(
        self,
        frame_store,
        camera_manager,
        on_incident_callback: Callable,
        broadcast_fn: Optional[Callable] = None,
        alert_engine = None,
        model_name: str = DEFAULT_DINOV2_MODEL,
        device: str = "cuda",
        default_threshold: float = DEFAULT_SCENE_THRESHOLD,
        semantic_interval: float = DEFAULT_SEMANTIC_INTERVAL,
        event_cooldown: float = DEFAULT_EVENT_COOLDOWN,
    ):
        self.frame_store = frame_store
        self.camera_manager = camera_manager
        self.on_incident_callback = on_incident_callback
        self.broadcast_fn = broadcast_fn
        self.alert_engine = alert_engine
        self.model_name = model_name
        self.device = device if torch.cuda.is_available() else "cpu"
        self.default_threshold = default_threshold
        self.semantic_interval = semantic_interval
        self.event_cooldown = event_cooldown

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._processor = None
        self._model = None

        # Per-camera state
        self._prev_embeddings: Dict[str, np.ndarray] = {}
        self._last_event_time: Dict[str, float] = {}
        self._last_sample_time: Dict[str, float] = {}
        self._active_collectors: Dict[str, asyncio.Task] = {}
        self.latest_drifts: Dict[str, float] = {}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        try:
            print(f"[SceneTrigger] Loading {self.model_name} on {self.device}...")
            self._processor = AutoImageProcessor.from_pretrained(self.model_name)
            self._model = AutoModel.from_pretrained(self.model_name).to(self.device)
            self._model.eval()
            print("[SceneTrigger] ✅ DINOv2 ready for scene shift detection")
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="SceneTrigger",
                effect=f"Failed to load DINOv2 model '{self.model_name}' on {self.device}; drift triggers disabled",
                severity="CRITICAL",
            )
            return

        self._task = asyncio.create_task(self._monitor_loop(), name="scene-trigger-loop")

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        for task in self._active_collectors.values():
            task.cancel()
        self._active_collectors.clear()

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                active_cams = self.camera_manager.get_active_cameras()
                now = time.monotonic()

                for cam_name in active_cams:
                    # Respect sampling interval per camera
                    if now - self._last_sample_time.get(cam_name, 0) < self.semantic_interval:
                        continue

                    # Don't sample if currently collecting post-trigger frames
                    if cam_name in self._active_collectors and not self._active_collectors[cam_name].done():
                        continue

                    latest = self.frame_store.get_latest(cam_name)
                    if latest is None:
                        continue

                    frame, frame_ts = latest
                    self._last_sample_time[cam_name] = now

                    # Run embedding inference in thread pool to avoid blocking asyncio event loop
                    emb = await asyncio.to_thread(self._extract_embedding, frame)
                    if emb is None:
                        continue

                    prev_emb = self._prev_embeddings.get(cam_name)
                    self._prev_embeddings[cam_name] = emb

                    if prev_emb is not None:
                        # Cosine distance = 1 - dot(prev, curr)
                        distance = float(1.0 - np.dot(prev_emb, emb))
                        self.latest_drifts[cam_name] = round(distance, 4)

                        # Check threshold and cooldown
                        threshold = self._get_cam_threshold(cam_name)
                        cooldown_elapsed = (now - self._last_event_time.get(cam_name, 0)) >= self.event_cooldown

                        if distance >= threshold and cooldown_elapsed:
                            self._last_event_time[cam_name] = now
                            t_trigger = time.monotonic()
                            print(
                                f"[SceneTrigger] 🚨 SCENE SHIFT on {cam_name}! "
                                f"Drift: {distance:.4f} (threshold: {threshold:.4f})"
                            )
                            # Start asynchronous post-trigger collection
                            self._active_collectors[cam_name] = asyncio.create_task(
                                self._collect_incident(cam_name, distance, frame_ts, trigger_time=t_trigger)
                            )

                # Broadcast live drift metrics to dashboard every 1s
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                error_tracker.capture_exception(
                    e,
                    component="SceneTrigger",
                    effect="Error in DINOv2 monitor loop; pausing for 0.5s",
                    severity="WARNING",
                )
                await asyncio.sleep(0.5)

    def _extract_embedding(self, frame: np.ndarray) -> Optional[np.ndarray]:
        try:
            # Resize frame down for fast feature extraction
            h, w = frame.shape[:2]
            target_w = DINOV2_INPUT_WIDTH
            target_h = int(h * (target_w / w))
            small = cv2.resize(frame, (target_w, target_h), interpolation=cv2.INTER_AREA)
            rgb = cv2.cvtColor(small, cv2.COLOR_BGR2RGB)
            img = Image.fromarray(rgb)

            inp = self._processor(images=img, return_tensors="pt").to(self.device)
            with torch.no_grad():
                out = self._model(**inp)

            emb = out.last_hidden_state.mean(dim=1).cpu().numpy()[0]
            norm = np.linalg.norm(emb)
            if norm > 0:
                emb = emb / norm
            return emb
        except Exception as e:
            error_tracker.capture_exception(
                e,
                component="SceneTrigger",
                effect="Failed to extract DINOv2 embedding; frame skipped",
                severity="WARNING",
            )
            return None

    def set_default_threshold(self, val: float) -> None:
        self.default_threshold = float(val)
        print(f"[SceneTrigger] Global default drift threshold updated to: {self.default_threshold:.4f}")

    def _get_cam_threshold(self, cam_name: str) -> float:
        cams = self.camera_manager.get_config()
        for c in cams:
            if c.get("name") == cam_name and c.get("threshold") is not None:
                try:
                    return float(c["threshold"])
                except (ValueError, TypeError) as exc:
                    error_tracker.capture_exception(
                        exc,
                        component="SceneTrigger",
                        camera=cam_name,
                        effect=f"Invalid threshold for {cam_name}; using global threshold {self.default_threshold}",
                        severity="WARNING",
                    )
        return float(self.default_threshold)

    async def _collect_incident(
        self,
        cam_name: str,
        drift_score: float,
        trigger_ts: float,
        trigger_time: Optional[float] = None,
    ) -> None:
        """
        Immediately collects 4 temporal frames over the past 10 seconds:
        - Frame 1: Scene baseline (t -10.0s)
        - Frame 2: Pre-motion development (t -5.0s)
        - Frame 3: Acceleration / trigger onset (t -2.0s)
        - Frame 4: Peak scene shift (t 0.0s / trigger moment)
        Concentrates 3/4 frames in the critical last 5-6 seconds.
        """
        try:
            if trigger_time is None:
                trigger_time = time.monotonic()

            # Retrieve temporal historical sequence: [-10.0, -5.0, -2.0]
            pre_frames = self.frame_store.get_pre_trigger_frames(
                cam_name,
                trigger_time=trigger_time,
                offsets=[-10.0, -5.0, -2.0],
            )
            latest_entry = self.frame_store.get_latest(cam_name)
            current_frame = latest_entry[0] if latest_entry else None

            all_raw_frames = []
            if pre_frames:
                all_raw_frames.extend(pre_frames)
            if current_frame is not None:
                all_raw_frames.append(current_frame)

            # Fallback if buffer does not have 10 seconds of history yet
            if len(all_raw_frames) < 4:
                sampled = self.frame_store.get_temporal_snapshots_b64(
                    cam_name, count=4, span_sec=10.0, max_w=512, quality=80
                )
                frames_b64 = sampled or []
                thumbs_b64 = self.frame_store.get_temporal_snapshots_b64(
                    cam_name, count=4, span_sec=10.0, max_w=TEMPORAL_THUMB_WIDTH, quality=TEMPORAL_THUMB_QUALITY
                ) or []
            else:
                frames_b64 = self.frame_store.encode_frames(all_raw_frames, max_w=512, quality=80)
                thumbs_b64 = self.frame_store.encode_frames(
                    all_raw_frames, max_w=TEMPORAL_THUMB_WIDTH, quality=TEMPORAL_THUMB_QUALITY
                )

            # Capture high-resolution snapshot for sharp modal preview
            high_res_snap = self.frame_store.get_snapshot_b64(
                cam_name, max_w=HIGH_RES_FRAME_WIDTH, quality=HIGH_RES_JPEG_QUALITY
            ) or (thumbs_b64[-1] if thumbs_b64 else None)

            labels = [
                "t -10.0s (Baseline)",
                "t -5.0s (Pre-incident)",
                "t -2.0s (Onset)",
                "t 0.0s (Trigger)",
            ]

            incident_data = {
                "cam": cam_name,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "trigger_ts": trigger_ts,
                "trigger_time": trigger_time,
                "drift": drift_score,
                "frames_b64": frames_b64,
                "thumbs_b64": thumbs_b64,
                "thumbnail_b64": high_res_snap,
                "labels": labels,
                "priority": 0,  # High priority incident
            }

            # Broadcast live scene drift shift to dashboard for live card pulse
            if self.broadcast_fn:
                await self.broadcast_fn({
                    "type": "scene_shift",
                    "cam": cam_name,
                    "drift": drift_score,
                    "timestamp": incident_data["timestamp"],
                    "thumbnail_b64": high_res_snap,
                    "thumbnails_b64": thumbs_b64,
                })

            # Queue for Cosmos Reason2 8B evaluation
            await self.on_incident_callback(incident_data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            error_tracker.capture_exception(
                e,
                component="SceneTrigger",
                camera=cam_name,
                effect=f"Error collecting incident temporal frames for {cam_name}; incident evaluation aborted",
                severity="ERROR",
            )
        finally:
            self._active_collectors.pop(cam_name, None)
