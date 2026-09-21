import json
import urllib.request
import urllib.parse
import time

BASE_URL = "http://localhost:7000"

def test_api_config():
    print("--- 1. Testing GET /api/config ---")
    req = urllib.request.Request(f"{BASE_URL}/api/config")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())
        sys_cfg = data.get("system", {})
        print("GET /api/config system keys:", list(sys_cfg.keys()))
        print("followup_interval_sec:", sys_cfg.get("followup_interval_sec"))
        print("persistent_followup:", sys_cfg.get("persistent_followup"))
        assert "followup_interval_sec" in sys_cfg, "Missing followup_interval_sec in system config"
        assert "persistent_followup" in sys_cfg, "Missing persistent_followup in system config"

    print("\n--- 2. Testing POST /api/config (dynamic update) ---")
    update_payload = {
        "followup_interval_sec": 8.0,
        "persistent_followup": True
    }
    req = urllib.request.Request(
        f"{BASE_URL}/api/config",
        data=json.dumps(update_payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        res = json.loads(resp.read().decode())
        print("POST response:", res)
        assert res["system"]["followup_interval_sec"] == 8.0
        assert res["system"]["persistent_followup"] is True

    # Check persistence in config/system.json
    with open("config/system.json") as f:
        persisted = json.load(f)
        assert persisted.get("followup_interval_sec") == 8.0
        assert persisted.get("persistent_followup") is True
        print("Verified config/system.json persisted correctly!")

    # Reset or set to 10s and persistent True for test
    update_payload2 = {
        "followup_interval_sec": 10.0,
        "persistent_followup": True
    }
    req2 = urllib.request.Request(
        f"{BASE_URL}/api/config",
        data=json.dumps(update_payload2).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req2) as resp:
        assert resp.status == 200
        print("Reset config to 10.0s and persistent_followup=True for follow-up testing.")

def test_frontend_serving():
    print("\n--- 3. Testing Frontend Assets Serving ---")
    req = urllib.request.Request(f"{BASE_URL}/")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        html = resp.read().decode()
        assert 'data-alert-filter="medium"' in html
        assert 'class="alert-filter-tab active" data-alert-filter="medium"' in html
        assert 'id="sys-input-followup-interval"' in html
        assert 'id="sys-input-persistent-followup"' in html
        assert 'app.js?v=22' in html
        print("Verified index.html contains default active medium filter tab, followup interval input, and persistent followup toggle!")

    req_js = urllib.request.Request(f"{BASE_URL}/app.js?v=22")
    with urllib.request.urlopen(req_js) as resp:
        assert resp.status == 200
        js = resp.read().decode()
        assert "activeAlertFilter: 'medium'" in js
        assert "sys-input-followup-interval" in js
        assert "sys-input-persistent-followup" in js
        assert "trigger_badge" in js
        print("Verified app.js serves latest code with activeAlertFilter: 'medium' and setting bindings!")

def test_trigger_alert():
    print("\n--- 4. Testing POST /api/alerts/test ---")
    req = urllib.request.Request(f"{BASE_URL}/api/alerts/test", data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        res = json.loads(resp.read().decode())
        alert = res.get("alert", {})
        print("Created test alert:")
        print(f"  ID: {alert.get('id')}")
        print(f"  Incident ID: {alert.get('incident_id')}")
        print(f"  Trigger Badge: {alert.get('trigger_badge')}")
        print(f"  Severity: {alert.get('severity')}")
        assert alert.get("id"), "Alert missing ID"
        assert alert.get("incident_id"), "Alert missing incident_id"
        assert alert.get("trigger_badge") == "⚡ TRIGGER", f"Unexpected trigger badge: {alert.get('trigger_badge')}"

if __name__ == "__main__":
    test_api_config()
    test_frontend_serving()
    test_trigger_alert()
    print("\n✅ All automated verification tests passed successfully!")
