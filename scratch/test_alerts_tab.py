import urllib.request, json

req = urllib.request.Request("http://localhost:7000/api/alerts?n=50")
with urllib.request.urlopen(req) as resp:
    alerts = json.loads(resp.read().decode("utf-8"))

print(f"Total alerts fetched: {len(alerts)}")

# Frontend filter simulation:
valid = [a for a in alerts if not a.get("is_drift") and not str(a.get("observation", "")).startswith("⚡ DINOv2")]
print(f"Valid alerts after DINOv2 filter: {len(valid)}")

counts = {"all": 0, "high_old": 0, "high_new": 0, "med_old": 0, "med_new": 0, "trigger_old": 0, "trigger_new": 0}

for a in valid:
    sev = (a.get("severity") or "low").lower()
    safety = (a.get("safety") or "").lower()
    is_followup = a.get("trigger_mode") == "FOLLOWUP" or a.get("is_followup", False)
    is_incident = bool(a.get("is_incident") or a.get("trigger_mode") == "TRIGGER") and not is_followup
    is_drift = bool(a.get("is_drift") or is_incident or (a.get("drift") is not None and a.get("drift") > 0))

    # Old logic:
    m_all = True
    m_high_old = sev in ("high", "extreme")
    m_med_old = sev in ("medium", "high", "extreme") or is_incident or is_followup
    m_trig_old = is_incident or is_followup or is_drift

    # Proper new logic:
    is_high = sev in ("high", "extreme", "critical") or safety == "danger"
    is_med_plus = is_high or sev == "medium" or safety == "warning"
    m_trig_new = is_incident or is_followup or is_drift

    if m_all: counts["all"] += 1
    if m_high_old: counts["high_old"] += 1
    if is_high: counts["high_new"] += 1
    if m_med_old: counts["med_old"] += 1
    if is_med_plus: counts["med_new"] += 1
    if m_trig_old: counts["trigger_old"] += 1
    if m_trig_new: counts["trigger_new"] += 1

print("\n--- COMPARISON OF FILTER COUNTS ---")
print(f"All:        {counts['all']}")
print(f"High (old): {counts['high_old']} | High (proper): {counts['high_new']}")
print(f"Med+ (old): {counts['med_old']} | Med+ (proper): {counts['med_new']}")
print(f"Trigger:    {counts['trigger_old']} | Trigger:       {counts['trigger_new']}")

# Sample low severity alerts that were showing in old Med+:
low_in_med_old = [a for a in valid if (a.get("severity") or "low").lower() == "low" and (
    (a.get("severity") or "low").lower() in ("medium", "high", "extreme") or bool(a.get("is_incident")) or bool(a.get("is_followup"))
)]
print(f"\nNumber of LOW severity alerts falsely showing in old Med+: {len(low_in_med_old)}")
if low_in_med_old:
    print(f"Example: ID={low_in_med_old[0].get('id')} sev={low_in_med_old[0].get('severity')} mode={low_in_med_old[0].get('trigger_mode')}")
