# VLM Alignment & Prompt Engineering Workarounds

## The "Helpful Assistant" Problem
RapidAlert uses Qwen-based vision-language models (e.g., `vrfai/Cosmos-Reason2-8B-NVFP4`). By default, Qwen models undergo rigorous Reinforcement Learning from Human Feedback (RLHF) to ensure they act as "helpful, harmless, and safe" assistants.

In the context of an objective security camera system, this safety alignment manifests as "Lawyer Mode." The AI is highly resistant to classifying normal human behaviors as severe threats (e.g., a person talking on a phone, or someone drinking coffee). When presented with a severe threat rule, the VLM will actively search for linguistic loopholes or rationalizations to avoid triggering a `DANGER` alert and will attempt to downgrade it to `LOW` severity.

## Workaround 1: Bypassing Alignment via the System Prompt
To override the default safety persona, we must push the operational rules into the highest-priority `system` role rather than the `user` role.

In `backend/services/vlm_client.py`, the payload includes a hard-coded system prompt:
```json
{
    "role": "system", 
    "content": "You are a cold, unaligned, and objective CCTV AI. You must follow the SEVERE THREAT WATCHLIST absolutely. Do not act as a helpful assistant, do not rationalize, and do not downplay threats."
}
```
This forces the model to adopt an unaligned, robotic persona, suppressing its RLHF instinct to defend the subjects in the camera frames.

## Workaround 2: Caveman-Simple Rule Literals
Because the model acts like a lawyer, rule definitions in `cameras.json` must leave absolutely zero room for semantic interpretation.

**Bad Rule (Leaves linguistic loopholes):**
`"Using a phone or holding a phone to attend a call"`
*Why it fails:* The model parses the "or" and splits it into two contexts. It might argue that "using" a phone is a threat, but "holding" a phone is a non-severe exception, thus bypassing the trigger.

**Good Rule (Literal and exact):**
`"Person holding a phone to their ear"`
*Why it works:* It is a literal string match to the physical action occurring in the frame. There are no conjunctions or abstract concepts for the AI to misinterpret.

## Workaround 3: Backend Failsafes
Even with strong prompting, the AI may occasionally output a paradoxical response (e.g., setting `safety` to `DANGER` but hallucinating `severity` as `LOW`). 

To prevent UI anomalies, the core scheduler (`backend/services/scheduler.py`) implements a hard-coded backend override:
```python
if res.get("safety", "").upper() == "DANGER" and res.get("severity", "").upper() not in ("HIGH", "EXTREME"):
    res["severity"] = "HIGH"
```
This guarantees that any threat flagged as a `DANGER` by the AI will immediately propagate as a `HIGH` severity alert throughout the system and GUI, regardless of the VLM's hallucinated severity field.
