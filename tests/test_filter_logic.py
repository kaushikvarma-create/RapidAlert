# Test filter matching logic from app.js

alerts = [
    {"sev": "low", "trigger": "periodic", "is_drift": False},
    {"sev": "medium", "trigger": "periodic", "is_drift": False},
    {"sev": "high", "trigger": "periodic", "is_drift": False},
    {"sev": "high", "trigger": "trigger", "is_drift": True},
    {"sev": "low", "trigger": "followup", "is_drift": False},
    {"sev": "medium", "trigger": "followup", "is_drift": False},
]

def check_filter(af, alert):
    sev = alert["sev"].lower()
    trigger = alert["trigger"]
    is_drift = alert["is_drift"]

    if af == "all":
        return True
    elif af == "high":
        return sev in ("high", "extreme")
    elif af == "medium":
        return sev in ("medium", "high", "extreme")
    elif af in ("drift", "trigger"):
        return trigger in ("trigger", "followup") or is_drift
    return False

# Test 'all':
all_results = [check_filter("all", a) for a in alerts]
assert all(all_results), "Filter 'all' should show everything"

# Test 'high':
high_results = [i for i, a in enumerate(alerts) if check_filter("high", a)]
assert high_results == [2, 3], f"Filter 'high' should only match high alerts, got {high_results}"

# Test 'medium' (Med+ default):
medium_results = [i for i, a in enumerate(alerts) if check_filter("medium", a)]
assert medium_results == [1, 2, 3, 5], f"Filter 'medium' should match medium, high, extreme, got {medium_results}"

# Test 'trigger':
trigger_results = [i for i, a in enumerate(alerts) if check_filter("trigger", a)]
assert trigger_results == [3, 4, 5], f"Filter 'trigger' should match triggers and followups, got {trigger_results}"

print("✅ Filter logic tests passed!")
