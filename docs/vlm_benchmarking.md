Yesterday 4:46 PM
lets say im benchmarking for both accuracy and speed to compare 2 vllms conatiners on same partition vs 1 vllm each on mig seperate, are there datasets with ground truth i cn use/ benchmarching programs/ methods?

Yes. And for your exact experiment — 2 VLM servers competing on the same Thor partition vs 1 VLM server per MIG instance — I would separate the benchmark into two layers:

Model-quality benchmark: same images/videos → compare JSON answers against ground truth.
Serving benchmark: same request stream → compare latency, throughput, queueing, GPU utilization, and tail latency.

The important thing is that a generic VLM benchmark like Video-MME is useful for validating model accuracy, but it won't tell you much about whether your particular two-container architecture is better for your surveillance workload.

1. For accuracy: use a ground-truth dataset

There are several options.

Benchmark	What it tests	Ground truth	Relevance to your system
Video-MME	Video understanding / temporal reasoning	Human annotated	⭐⭐⭐⭐
LVBench	Long-video understanding	Annotated QA	⭐⭐⭐
ShareGPT4V	Image understanding / captioning	Human/generated annotations	⭐⭐⭐
VisionArena	Multimodal conversations	Human preference data	⭐⭐
Your own surveillance dataset	Exactly your use case	Yes	⭐⭐⭐⭐⭐

Video-MME is particularly convenient because it has 900 videos, 2,700 human-annotated QA pairs, and an evaluation script that produces accuracy by video duration, domain, and task type.

LVBench is also interesting because it specifically includes everyday surveillance footage among its long-video material.

But I'd actually make your own benchmark dataset the primary accuracy benchmark.

2. Your own dataset can be much better

You already have the perfect structure for this.

Suppose your VLM receives:

CAMERA: PARKING

IMAGE: frame_000123.jpg

PROMPT:
Describe the current state of the scene using the required JSON schema.

You create:

{
  "image": "frame_000123.jpg",

  "ground_truth": {
    "people": 3,
    "vehicles": 2,
    "activity": "normal",
    "person_locations": ["entrance", "vehicle_1", "vehicle_2"],
    "abnormal_activity": false
  }
}

Then both VLLMs get bit-for-bit identical inputs.

You measure:

                 Same Dataset
                     │
          ┌──────────┴──────────┐
          ↓                     ↓
     VLLM A                  VLLM B
   same partition          MIG partition
          │                     │
          ↓                     ↓
      JSON A                  JSON B
          │                     │
          └──────────┬──────────┘
                     ↓
              Ground Truth
                     ↓
             Accuracy metrics

This is much more meaningful than simply saying:

"Container A produced the answer in 1.4 seconds and Container B in 1.2 seconds."

3. You should benchmark accuracy at multiple levels

Because your system produces structured scene descriptions, don't use only one "accuracy" number.

For example:

Object-level
GT people:       3
VLM:             3

GT vehicles:     2
VLM:             2

Measure:

precision
recall
F1
counting error
Attribute-level
GT:
person_1 = standing
person_2 = walking
person_3 = sitting

VLM:
person_1 = standing
person_2 = standing
person_3 = sitting

You get:

2/3 correct = 66.7%
Event-level

This is probably more important for your system:

GROUND TRUTH:
normal

VLM:
normal

versus:

GROUND TRUTH:
person enters restricted area

VLM:
person enters restricted area

Measure:

event detection recall
false positive rate
false negative rate
JSON validity

Also measure:

valid JSON
required fields present
enum values valid
schema violations
hallucinated objects

This is extremely relevant to your architecture.

A VLM that is 95% semantically correct but produces malformed JSON 10% of the time isn't equivalent operationally to one producing valid structured output 99.9% of the time.

4. Then separately benchmark SPEED

This is where vLLM's own benchmark tooling becomes useful.

Current vLLM has:

vllm bench latency
vllm bench serve
vllm bench throughput
multimodal processor benchmarking
percentile latency reporting

and specifically supports multimodal benchmark datasets.

For your use case, I'd primarily use:

vllm bench serve

because you're interested in actual serving behavior rather than only raw model inference.

It reports things like:

TTFT
TPOT
ITL
request throughput
token throughput
latency percentiles

including p99.

5. But here's the important part for YOUR experiment

Don't benchmark:

1 request
1 request
1 request

That's not representative of your surveillance system.

You want something like:

Camera 1 ──┐
Camera 2 ──┤
Camera 3 ──┤
Camera 4 ──┼──→ request generator
Camera 5 ──┤
Camera 6 ──┤
Camera 7 ──┘

with requests arriving continuously.

For example:

7 cameras
1 request every 4 seconds per camera

= 1.75 requests/sec aggregate

Then test:

Configuration A
Thor
└── GPU partition
    ├── VLLM-1
    └── VLLM-2
Configuration B
Thor
├── MIG instance 1
│   └── VLLM-1
│
└── MIG instance 2
    └── VLLM-2

And importantly:

feed both configurations exactly the same request trace.

6. Record this for every request

I'd build a CSV like:

request	camera	send_time	first_token	finish	input_tokens	output_tokens	JSON valid	accuracy
001	CAM1	0.000	.421	1.82	1450	180	✓	96%
002	CAM2	0.000	.438	1.91	1420	190	✓	94%
003	CAM3	4.000	.429	1.76	1500	170	✓	100%

Then calculate:

Speed
mean latency
median latency
p90
p95
p99
max latency
requests/sec
images/sec
tokens/sec
Quality
JSON validity
field accuracy
object F1
event recall
false-positive rate
false-negative rate
System
GPU utilization
GPU memory
SM utilization
memory bandwidth
CPU utilization
RAM
queue depth
GPU temperature
power
7. One metric I REALLY want you to measure

For your surveillance system:

Deadline success rate

Suppose your application needs each image analysed within 4 seconds.

Then:

deadline = 4 sec

If you send:

1000 images

and:

947 completed < 4 sec
53 completed > 4 sec

then:

Deadline success = 94.7%

That's potentially much more useful to you than average latency.

Because:

Configuration A
mean = 1.4 sec
p99 = 7.8 sec

could actually be worse operationally than:

Configuration B
mean = 1.7 sec
p99 = 2.4 sec

if your cameras need predictable periodic analysis.

8. Also test degradation under load

I'd run at least:

0.25 req/s
0.5 req/s
1 req/s
1.5 req/s
2 req/s
2.5 req/s
3 req/s

for both architectures.

You may get a graph like:

p99 latency
│
│                         Same partition
│                       /
│                     /
│                   /
│             _____/
│       _____/
│______/________________ MIG
│
└────────────────────────────
          request rate

This tells you where each configuration starts falling over.

That's far more informative than a single benchmark run.

9. MIG is particularly interesting here

NVIDIA's MIG design is specifically intended to provide isolated GPU resources, including isolated paths through memory/cache/controller resources, so workloads on separate instances are designed to have more predictable interference characteristics.

So your experiment is actually testing an important question:

Does the better resource isolation of MIG compensate for the reduced resources available to each individual VLLM instance?

That's exactly the experiment I'd run.

You might discover:

                    Same partition      MIG
------------------------------------------------
VLLM-1 latency          1.2 s            1.5 s
VLLM-2 latency          1.3 s            1.5 s
p99                     4.8 s            2.1 s
aggregate throughput    1.8 req/s        1.7 req/s
deadline success        88%              98%

In that hypothetical case, MIG could give you more predictable service despite lower raw throughput.

Or you could get the opposite.

That's why measuring only average inference speed won't answer your question.

10. There is also an existing benchmark framework worth using

vLLM itself recommends GuideLLM for production-style serving benchmarks; it supports workload patterns beyond the simpler vllm bench serve tooling.

I'd use:

vLLM bench → quick controlled experiments

GuideLLM → serious load/serving experiments

Your own Python harness → surveillance-specific accuracy + request-trace benchmark.

What I'd actually build for your Thor

I'd make one benchmark program with three components:

                    BENCHMARK CONTROLLER
                           │
             ┌─────────────┴─────────────┐
             │                           │
       Request Generator            Ground Truth
             │                           │
             ↓                           │
      identical image                  │
      request sequence                 │
             │                           │
       ┌─────┴─────┐                     │
       ↓           ↓                     │
    VLLM A       VLLM B                  │
       │           │                     │
       └─────┬─────┘                     │
             ↓                           ↓
        Result Collector ───────→ Accuracy Engine
             │
             ↓
        Performance DB
             │
             ↓
      ┌──────┴────────┐
      ↓               ↓
   Accuracy         Latency
   throughput       p50/p95/p99
   GPU metrics      queue depth

Then run the exact same test suite against:

TEST 1
2 × VLLM
same GPU partition

and:

TEST 2
1 × VLLM
MIG instance A

1 × VLLM
MIG instance B

and repeat each test at least 3–5 times.

That will give you a genuinely defensible answer about whether your Thor configuration is better for your camera workload, rather than just a synthetic benchmark score.

If you want, I can also design the actual benchmark harness for your Qwen3-VL setup — including the JSON ground-truth format, request generator, p50/p95/p99 calculations, GPU telemetry, and a final A-vs-B report so you can just point it at your two vLLM endpoints.
