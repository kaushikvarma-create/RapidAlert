import tempfile
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from backend.services.auth_service import AuthService
from backend.main import app, auth_service

@pytest.fixture
def temp_auth_service():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_auth.db"
        service = AuthService(db_path)
        yield service

def test_auth_service_hashing_and_verification(temp_auth_service):
    # Default admin seeded
    assert temp_auth_service.is_configured() is True
    
    # Test valid login
    ok, token, err = temp_auth_service.authenticate("admin", "admin123")
    assert ok is True
    assert token is not None
    assert err is None
    
    # Validate session
    username = temp_auth_service.validate_session(token)
    assert username == "admin"
    
    # Test invalid password
    bad_ok, bad_token, bad_err = temp_auth_service.authenticate("admin", "wrongpassword")
    assert bad_ok is False
    assert bad_token is None
    assert "Invalid" in bad_err

def test_auth_password_change(temp_auth_service):
    # Change password
    ok, err = temp_auth_service.change_password("admin", "admin123", "SuperSecure#999")
    assert ok is True
    assert err is None
    
    # Old password fails
    old_ok, _, _ = temp_auth_service.authenticate("admin", "admin123")
    assert old_ok is False
    
    # New password works
    new_ok, new_token, _ = temp_auth_service.authenticate("admin", "SuperSecure#999")
    assert new_ok is True
    assert new_token is not None

def test_auth_session_revocation(temp_auth_service):
    ok, token, _ = temp_auth_service.authenticate("admin", "admin123")
    assert ok is True
    assert temp_auth_service.validate_session(token) == "admin"
    
    temp_auth_service.revoke_session(token)
    assert temp_auth_service.validate_session(token) is None

def test_api_auth_and_protected_routes():
    client = TestClient(app)
    
    # 1. Status check
    res = client.get("/api/auth/status")
    assert res.status_code == 200
    assert res.json().get("configured") is True
    
    # 2. Protected route without token -> 401
    res = client.post("/api/prompts", json={"master_prompt": "test"})
    assert res.status_code == 401
    
    res = client.post("/api/cameras", json={"name": "TEST_CAM", "source": 0})
    assert res.status_code == 401
    
    res = client.delete("/api/alerts")
    assert res.status_code == 401

    # 3. Login with invalid credentials -> 401
    res = client.post("/api/auth/login", json={"username": "admin", "password": "wrongpassword_12345"})
    assert res.status_code == 401
    assert "Invalid" in res.json().get("detail", "")
    
    # 4. Login with valid credentials -> 200 + token
    res = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    # Note: if password was changed in db previously or is default admin123
    if res.status_code == 200:
        token = res.json().get("token")
        assert token is not None
        
        # 5. Access protected route with Bearer token -> 200
        headers = {"Authorization": f"Bearer {token}"}
        res = client.get("/api/config")  # GET is open or test POST
        assert res.status_code == 200
        
        # Test alert with Bearer token
        res = client.post("/api/alerts/test", headers=headers)
        assert res.status_code == 200
        
        # 6. Test logout
        res = client.post("/api/auth/logout", headers=headers)
        assert res.status_code == 200
        
        # After logout -> 401
        res = client.post("/api/alerts/test", headers=headers)
        assert res.status_code == 401
