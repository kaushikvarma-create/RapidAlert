"""
SceneTriggerEngine: DINOv2-based lightweight real-time scene change detector.
Runs facebook/dinov2-small on CUDA at 1-2 FPS per active camera.
Computes cosine embedding drift between consecutive samples.
When drift exceeds the threshold, captures pre-trigger context, awaits post-trigger progression,
and dispatches an Incident Event to the Scheduler for Cosmos Reason2 8B evaluation.
"""
import asyncio
import time
from typing import Callable, Dict, List, Optional
import cv2
import numpy as np
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModel


class SceneTriggerEngine:
    def __init__(
        self,
        frame_store,
        camera_manager,
        on_incident_callback: Callable,
        broadcast_fn: Optional[Callable] = None,
        alert_engine = None,
        model_name: str = "facebook/dinov2-small",
        device: str = "cuda",
        default_threshold: float = 0.033,
        semantic_interval: float = 0.5,
        event_cooldown: float = 15.0,
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
        print(f"[SceneTrigger] Loading {self.model_name} on {self.device}...")
        self._processor = AutoImageProcessor.from_pretrained(self.model_name)
        self._model = AutoModel.from_pretrained(self.model_name).to(self.device)
        self._model.eval()
        print("[SceneTrigger] ✅ DINOv2 ready for scene shift detection")

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
                print(f"[SceneTrigger] Error in loop: {e}")
                await asyncio.sleep(0.5)

    def _extract_embedding(self, frame: np.ndarray) -> Optional[np.ndarray]:
        try:
            # Resize frame down for fast feature extraction
            h, w = frame.shape[:2]
            target_w = 448
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
            print(f"[SceneTrigger] Embedding error: {e}")
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
                except (ValueError, TypeError):
                    pass
        return float(self.default_threshold)

    async def _collect_incident(
        self,
        cam_name: str,
        drift_score: float,
        trigger_ts: float,
        trigger_time: Optional[float] = None,
    ) -> None:
        """
        Asynchronously collects pre-trigger and post-trigger frames.
        t0 (pre-trigger):  t-4.0s, t-1.0s
        t1 (post-trigger): awaits t+1.5s, t+3.5s
        Total 4 temporal frames representing the full incident evolution.
        """
        if trigger_time is None:
            trigger_time = time.monotonic()
        try:
            # Extract t0 pre-trigger frames from rolling buffer
            t0_frames = self.frame_store.get_pre_trigger_frames(
                cam_name, trigger_ts, offsets=[-4.0, -1.0]
            )

            # Wait and collect post-trigger frame 1 (t+1.5s)
            await asyncio.sleep(1.5)
            post1 = self.frame_store.get_latest(cam_name)
            t1_frame1 = post1[0].copy() if post1 else (t0_frames[-1] if t0_frames else None)

            # Wait and collect post-trigger frame 2 (t+3.5s)
            await asyncio.sleep(2.0)
            post2 = self.frame_store.get_latest(cam_name)
            t1_frame2 = post2[0].copy() if post2 else (t1_frame1 if t1_frame1 is not None else None)

            # Assemble the 4 frames
            frames = []
            if len(t0_frames) >= 2:
                frames.extend(t0_frames)
            elif len(t0_frames) == 1:
                frames.extend([t0_frames[0], t0_frames[0]])
            else:
                fallback = self.frame_store.get_latest(cam_name)
                frames.extend([fallback[0], fallback[0]] if fallback else [])

            if t1_frame1 is not None:
                frames.append(t1_frame1)
            if t1_frame2 is not None:
                frames.append(t1_frame2)

            # Base64 encode for VLM (downscaled to 512 for fast multi-frame processing)
            frames_b64 = self.frame_store.encode_frames(frames, max_w=512, quality=75)
            # Crisp temporal sequence frames (480px) for event timeline inspection
            thumbs_b64 = self.frame_store.encode_frames(frames, max_w=480, quality=68)
            # High resolution snapshot (960px, quality 78) for sharp incident inspector
            high_res_snap = self.frame_store.encode_frames([frames[-1]], max_w=960, quality=78)[0] if frames else (thumbs_b64[-1] if thumbs_b64 else None)

            labels = ["t -4.0s (Before)", "t -1.0s (Trigger)", "t +1.5s (Action)", "t +3.5s (Outcome)"]

            incident_data = {
                "cam": cam_name,
                "drift": drift_score,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "epoch": time.time(),
                "trigger_time": trigger_time,
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
            print(f"[SceneTrigger] Error collecting incident for {cam_name}: {e}")
        finally:
            self._active_collectors.pop(cam_name, None)
