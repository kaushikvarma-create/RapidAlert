"""
Test all alert filtering scenarios to verify:
1. 'all': shows all alerts
2. 'high': shows only HIGH / EXTREME / DANGER
3. 'medium' (Med+): shows only MEDIUM, HIGH, EXTREME, WARNING, DANGER (NEVER LOW / OK)
4. 'trigger': shows only TRIGGER and FOLLOWUP events
"""

def matches_filter(alert, filter_name):
    if not alert:
        return False
    af = filter_name or 'medium'
    if af == 'all':
        return True

    sev = str(alert.get('severity') or 'low').strip().lower()
    safety = str(alert.get('safety') or '').strip().lower()
    is_high = sev in ('high', 'extreme', 'critical') or safety == 'danger'
    is_med_plus = is_high or sev == 'medium' or safety == 'warning'

    if af == 'high':
        return is_high
    if af == 'medium':
        return is_med_plus
    if af in ('trigger', 'drift'):
        trigger_mode = str(alert.get('trigger_mode') or '').upper()
        is_trigger = trigger_mode == 'TRIGGER' or bool(alert.get('is_incident'))
        is_followup = trigger_mode == 'FOLLOWUP' or bool(alert.get('is_followup'))
        is_drift = bool(alert.get('is_drift') or (alert.get('drift') is not None and alert.get('drift') > 0))
        return is_trigger or is_followup or is_drift
    return True

test_alerts = [
    # 1. Periodic low severity check
    {"id": "A1", "severity": "LOW", "safety": "OK", "trigger_mode": "PERIODIC", "is_incident": False, "is_followup": False},
    # 2. Scene drift trigger with Medium severity
    {"id": "A2", "severity": "MEDIUM", "safety": "WARNING", "trigger_mode": "TRIGGER", "is_incident": True, "is_followup": False},
    # 3. Follow-up outcome check that resolved to LOW
    {"id": "A3", "severity": "LOW", "safety": "OK", "trigger_mode": "FOLLOWUP", "is_incident": True, "is_followup": True},
    # 4. Critical High severity incident
    {"id": "A4", "severity": "HIGH", "safety": "DANGER", "trigger_mode": "TRIGGER", "is_incident": True, "is_followup": False},
    # 5. Follow-up outcome check that stayed HIGH
    {"id": "A5", "severity": "HIGH", "safety": "DANGER", "trigger_mode": "FOLLOWUP", "is_incident": True, "is_followup": True},
    # 6. Periodic check with Warning / Medium
    {"id": "A6", "severity": "MEDIUM", "safety": "WARNING", "trigger_mode": "PERIODIC", "is_incident": False, "is_followup": False},
]

# Scenario 1: 'all' filter
res_all = [a["id"] for a in test_alerts if matches_filter(a, 'all')]
print("Filter 'all':", res_all)
assert res_all == ["A1", "A2", "A3", "A4", "A5", "A6"], f"Failed 'all': {res_all}"

# Scenario 2: 'high' filter
res_high = [a["id"] for a in test_alerts if matches_filter(a, 'high')]
print("Filter 'high':", res_high)
assert res_high == ["A4", "A5"], f"Failed 'high': {res_high}"

# Scenario 3: 'medium' (Med+) filter
res_med = [a["id"] for a in test_alerts if matches_filter(a, 'medium')]
print("Filter 'medium' (Med+):", res_med)
# A1 (LOW) and A3 (LOW follow-up) must NOT be in Med+!
assert "A1" not in res_med, "A1 (LOW) should not be in Med+"
assert "A3" not in res_med, "A3 (LOW follow-up) should not be in Med+"
assert res_med == ["A2", "A4", "A5", "A6"], f"Failed 'medium': {res_med}"

# Scenario 4: 'trigger' filter
res_trig = [a["id"] for a in test_alerts if matches_filter(a, 'trigger')]
print("Filter 'trigger':", res_trig)
# Periodic checks A1 and A6 must NOT be in trigger!
assert "A1" not in res_trig, "A1 (PERIODIC) should not be in trigger"
assert "A6" not in res_trig, "A6 (PERIODIC) should not be in trigger"
assert res_trig == ["A2", "A3", "A4", "A5"], f"Failed 'trigger': {res_trig}"

print("\n🎉 ALL FILTER SCENARIOS PASSED WITH 100% ACCURACY!")
