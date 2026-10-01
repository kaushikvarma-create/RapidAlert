"""
Tests for Incident Frame Archive and Rolling Buffer in storage.py
"""
import os
import tempfile
import base64
from pathlib import Path
import pytest
from backend.services.storage import StorageManager

@pytest.fixture
def temp_storage():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    storage = StorageManager(db_path=db_path)
    yield storage
    if db_path.exists():
        os.unlink(db_path)

def test_save_and_retrieve_incident_frames(temp_storage):
    # Dummy base64 images
    dummy_bytes = [b"FRAME_0_DATA", b"FRAME_1_DATA", b"FRAME_2_DATA", b"FRAME_3_DATA"]
    dummy_b64 = [base64.b64encode(b).decode("utf-8") for b in dummy_bytes]
    
    saved_count = temp_storage.save_incident_frames(
        incident_id="inc_001",
        event_id="evt_001",
        cam="CAM_FRONT",
        frames=dummy_b64,
        ts=1700000000.0
    )
    assert saved_count == 4
    
    # Retrieve base64 frames
    frames = temp_storage.get_incident_frames("inc_001")
    assert len(frames) == 4
    for idx, f in enumerate(frames):
        assert f["frame_idx"] == idx
        assert f["incident_id"] == "inc_001"
        assert f["cam"] == "CAM_FRONT"
        assert f["b64"] == dummy_b64[idx]
        
    # Retrieve raw binary frame
    raw_0 = temp_storage.get_incident_frame_raw("inc_001", 0)
    assert raw_0 == b"FRAME_0_DATA"
    
    raw_none = temp_storage.get_incident_frame_raw("inc_001", 99)
    assert raw_none is None

def test_archive_stats(temp_storage):
    dummy_b64 = [base64.b64encode(b"FRAME").decode("utf-8")]
    temp_storage.save_incident_frames("inc_1", "evt_1", "CAM_1", dummy_b64, ts=1700000000.0)
    temp_storage.save_incident_frames("inc_2", "evt_2", "CAM_2", dummy_b64, ts=1700001000.0)
    
    stats = temp_storage.get_archive_stats()
    assert stats["total_sets"] == 2
    assert stats["total_frames"] == 2
    assert stats["earliest_ts"] == 1700000000.0
    assert stats["latest_ts"] == 1700001000.0

def test_rolling_buffer_eviction(temp_storage):
    dummy_b64 = [base64.b64encode(b"FRAME").decode("utf-8")]
    
    # Save 10 sets
    for i in range(10):
        temp_storage.save_incident_frames(f"inc_{i}", f"evt_{i}", "CAM_1", dummy_b64, ts=1700000000.0 + i)
        
    stats = temp_storage.get_archive_stats()
    assert stats["total_sets"] == 10
    
    # Prune keeping only top 3 newest sets
    deleted = temp_storage.prune_incident_frames(max_sets=3)
    assert deleted == 7
    
    stats_after = temp_storage.get_archive_stats()
    assert stats_after["total_sets"] == 3
    # Earliest remaining should be inc_7 (ts 1700000007)
    assert stats_after["earliest_ts"] == 1700000007.0


def test_rolling_buffer_size_guard(temp_storage):
    dummy_bytes = [b"FRAME_DATA" * 50] * 4
    dummy_b64 = [base64.b64encode(b).decode("utf-8") for b in dummy_bytes]
    for i in range(10):
        temp_storage.save_incident_frames(f"size_inc_{i}", f"evt_{i}", "CAM_1", dummy_b64, ts=1700000000.0 + i)

    # Calling with max_gb=0.000001 (threshold below current DB size) triggers size-based pruning
    deleted = temp_storage.prune_incident_frames(max_sets=1000, max_gb=0.000001)
    assert deleted > 0
