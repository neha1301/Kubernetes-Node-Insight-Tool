# Kubernetes Node Insight Tool   
### Understand your Kubernetes cluster in one run — no dashboards, no alerts, no guesswork.

A **human-first Kubernetes debugging and exploration tool** that **correlates node health, capacity, and workload behavior** into a single, readable output.

If you’ve ever asked *“why are pods evicted?”*, *“is this a node issue or an app issue?”*, or *“what does this cluster actually look like?”* — this tool is for you.

## Why this tool exists

In Kubernetes, a node can remain in a `Ready` state even after serious issues like kernel-level Out-Of-Memory (OOM) events. These conditions indicate instability but are easy to miss because:

- They do not always transition the node to `NotReady`
- They are buried in kubelet events
- Standard dashboards focus on pod metrics, not node internals

This tool correlates node conditions and kubelet events to produce a clear, actionable diagnosis.
---

## Features

- Cluster-wide node inspection
- Detection of system-level OOM events (`SystemOOM`)
- Detection of pod-level OOM kills
- Identification of resource pressure conditions:
  - MemoryPressure
  - DiskPressure
  - PIDPressure
- Root cause determination based on severity
- Diagnoses both:
  - NotReady nodes
  - Ready but degraded nodes
- Read-only and safe for clusters

---

## What the tool detects

### System-level OOM events
Detects kernel OOM killer events reported by kubelet, for example:

System OOM encountered, culprit process: stress, pid: 56239
---

These events are treated as high-severity signals even if the node remains Ready.

### Pod-level OOM kills
Detects OOMKilled containers and correlates them with the node.

### Resource pressure
Reads node conditions directly from the Kubernetes API:
- MemoryPressure
- DiskPressure
- PIDPressure

### Root cause summary
Determines the most likely root cause using a priority-based evaluation:
- System OOM events
- Memory pressure
- Disk pressure
- Repeated pod OOM kills

---

## Prerequisites

- Python 3.8 or newer
- Access to a Kubernetes cluster
- Valid kubeconfig or in-cluster configuration
- Python Kubernetes client

Install dependencies:

```bash
pip install kubernetes tabulate
```

## Example Output

```bash

======================================================================
CLUSTER INSIGHT SUMMARY
======================================================================
Nodes: 1
Karpenter-managed: 0
NotReady: 0
DiskPressure: 0
MemoryPressure: 0
PIDPressure: 0
Nodes with Evictions: 1
Nodes with OOM Kills: 0
Total OOM Events (past 7d): 0

======================================================================
NODE CAPACITY OVERVIEW
======================================================================
+-----------------+---------+---------------------+---------------------+
| Instance Type   |   Nodes |   Total CPU (cores) |   Total Memory (Gi) |
+=================+=========+=====================+=====================+
| k3s             |       1 |                   8 |                 3.9 |
+-----------------+---------+---------------------+---------------------+

Cluster Total: 1 nodes | 8.0 cores | 3.9 Gi

======================================================================
NODE: orbstack
======================================================================
Age: 36.0d | Ready: True | Cordoned: False
Instance: k3s | Capacity: unknown | AZ: unknown
Karpenter: False

 RESOURCES:
  Allocatable: 8.00 cores, 3.88 Gi
  DiskPressure: False
  MemoryPressure: False

 STABILITY (past 7d):
  Last Ready State Change: 0d ago
  Ready State Changes: 1
   Some instability detected

🔧 WORKLOADS:
  Evicted Pods: 2

  IMPACTING PODS (2):
  1. devtroncd/argocd-dex-server-f8d4c7fcd-6zvtx
     Status: Failed: Evicted
  2. devtroncd/argocd-dex-server-f8d4c7fcd-ckq9z
     Status: Failed: Evicted

 NOTES:
  •  Recent state change detected (0d ago)
```
