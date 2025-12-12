# DAS + Kueue Integration

This document provides a comprehensive overview of how the Dynamic Accelerator Slicer (DAS) operator integrates with Kueue for GPU quota management and workload scheduling.

## Table of Contents
1. [Overview](#overview)
2. [Why gpu.das.openshift.io/mem?](#why-gpudasopenshiftiomem)
3. [DAS Architecture](#das-architecture)
4. [Kueue Integration Flow](#kueue-integration-flow)
5. [Supported Workload Types](#supported-workload-types)
6. [Sequence Diagrams](#sequence-diagrams)
7. [Known Limitations](#known-limitations)
8. [Implementation Status](#implementation-status)
9. [Setup and Configuration](#setup-and-configuration)

---

## Overview

DAS (Dynamic Accelerator Slicer) enables **dynamic MIG partitioning** - creating GPU slices on-demand when workloads are scheduled, rather than pre-partitioning GPUs. Kueue provides job queuing and quota management for Kubernetes workloads.

The integration enables:
- **Unified GPU memory quota** - Track GPU resources as memory (GB) rather than individual MIG profiles
- **Dynamic slice creation** - Create MIG slices when pods are scheduled, not before
- **Multi-workload support** - Jobs, PyTorchJob, RayJob, MPIJob, JobSet, and more

---

## Why gpu.das.openshift.io/mem?

### The Fundamental Problem

**Q: Why can't Kueue just track `nvidia.com/mig-1g.10gb` directly?**

**A: Because with dynamic MIG, those resources don't exist until DAS creates them.**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    STATIC MIG vs DYNAMIC MIG (DAS)                      │
├─────────────────────────────────┬───────────────────────────────────────┤
│         STATIC MIG              │           DYNAMIC MIG (DAS)           │
├─────────────────────────────────┼───────────────────────────────────────┤
│                                 │                                       │
│  GPU pre-partitioned:           │  GPU unpartitioned:                   │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐   │  ┌─────────────────────────────────┐  │
│  │1g  │ │1g  │ │1g  │ │1g  │   │  │                                 │  │
│  │10gb│ │10gb│ │10gb│ │10gb│   │  │   80GB UNPARTITIONED MEMORY     │  │
│  └────┘ └────┘ └────┘ └────┘   │  │                                 │  │
│                                 │  └─────────────────────────────────┘  │
│  Node reports:                  │  Node reports:                        │
│  nvidia.com/mig-1g.10gb: 4     │  nvidia.com/mig-1g.10gb: 0 (!)       │
│                                 │                                       │
│  ✓ Kueue can track directly    │  ✗ Resource doesn't exist yet        │
│                                 │                                       │
├─────────────────────────────────┴───────────────────────────────────────┤
│                                                                         │
│  SOLUTION: DAS introduces gpu.das.openshift.io/mem                      │
│                                                                         │
│  - Represents GPU memory CAPACITY (not created slices)                  │
│  - Exists BEFORE slices are created                                     │
│  - Allows Kueue to make admission decisions                             │
│  - DAS scheduler creates actual slices when pods are scheduled          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Insight

| Resource Type | When it exists | What it represents |
|---------------|----------------|-------------------|
| `nvidia.com/mig-1g.10gb` | After slice is created | Actual MIG slice |
| `gpu.das.openshift.io/mem` | Always (before scheduling) | Allocatable GPU memory capacity |

### Benefits

1. **Unified Quota** - One quota for all MIG profiles (e.g., 80GB) instead of separate quotas per profile
2. **Profile Flexibility** - Users can request any profile; DAS creates it on-demand
3. **Better Utilization** - No wasted pre-allocated slices
4. **Simplified Admin** - One number to manage, not quotas for each profile type

---

## DAS Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DAS OPERATOR COMPONENTS                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
│  │  DAS Operator   │  │  DAS Webhook    │  │    DAS Scheduler        │ │
│  │  (Controller)   │  │  (Mutating)     │  │    (das-scheduler)      │ │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────────────┤ │
│  │                 │  │ Transforms:     │  │                         │ │
│  │ Manages CRDs:   │  │ • Pod           │  │ Scheduling:             │ │
│  │ • NodeAccel     │  │ • Job           │  │ • Filter by GPU memory  │ │
│  │ • AllocClaim    │  │ • PyTorchJob    │  │ • Score by utilization  │ │
│  │                 │  │ • MPIJob        │  │ • Read mig-profiles     │ │
│  │ Reconciles      │  │ • TFJob         │  │   annotation            │ │
│  │ allocations     │  │ • RayJob        │  │ • Create AllocationClaim│ │
│  │                 │  │ • JobSet        │  │ • Bind pod to node      │ │
│  │                 │  │ • Workload      │  │                         │ │
│  │                 │  │ • etc.          │  │                         │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────┘ │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      DAS Daemonset                               │   │
│  ├─────────────────────────────────────────────────────────────────┤   │
│  │ Per-node agent:                                                  │   │
│  │ • Discovers GPUs via nvidia-smi                                  │   │
│  │ • Creates/updates NodeAccelerator CR                             │   │
│  │ • Watches AllocationClaims                                       │   │
│  │ • Executes MIG slice creation (nvidia-smi mig -cgi/-ci)         │   │
│  │ • Reports gpu.das.openshift.io/mem capacity                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Webhook Architecture

The DAS webhook handles multiple resource types:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       DAS WEBHOOK HANDLERS                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SOURCE RESOURCE              WEBHOOK HANDLER           TRANSFORMATION  │
│  ───────────────              ───────────────           ──────────────  │
│                                                                         │
│  Pod ─────────────────────► webhook.go ──────────────┐                 │
│  Job ─────────────────────► job_webhook.go ──────────┤                 │
│  PyTorchJob ──────────────► kubeflow_webhook.go ─────┤                 │
│  MPIJob ──────────────────► kubeflow_webhook.go ─────┤                 │
│  TFJob ───────────────────► kubeflow_webhook.go ─────┼──► TransformPodTemplateForDAS()
│  XGBoostJob ──────────────► kubeflow_webhook.go ─────┤       │         │
│  PaddleJob ───────────────► kubeflow_webhook.go ─────┤       │         │
│  JAXJob ──────────────────► kubeflow_webhook.go ─────┤       ▼         │
│  RayJob ──────────────────► ray_webhook.go ──────────┤  ┌───────────┐  │
│  RayCluster ──────────────► ray_webhook.go ──────────┤  │ Mutated   │  │
│  JobSet ──────────────────► jobset_webhook.go ───────┤  │ Resource  │  │
│  AppWrapper ──────────────► appwrapper_webhook.go ───┤  └───────────┘  │
│  Deployment ──────────────► longrunning_webhook.go ──┤                 │
│  StatefulSet ─────────────► longrunning_webhook.go ──┤                 │
│  Workload (Kueue) ────────► workload_webhook.go ─────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### TransformPodTemplateForDAS()

The core transformation function (in `template_mutator.go`):

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 TransformPodTemplateForDAS()                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  INPUT (User's Pod Template):         OUTPUT (Transformed):             │
│  ────────────────────────────         ─────────────────────             │
│                                                                         │
│  containers:                          containers:                       │
│  - resources:                         - resources:                      │
│      limits:                              limits:                       │
│        nvidia.com/mig-1g.10gb: 1  ──►     gpu.das.openshift.io/mem: 10 │
│                                                                         │
│  spec:                                spec:                             │
│    schedulerName: default      ──►      schedulerName: das-scheduler   │
│                                         runtimeClassName: nvidia-legacy│
│                                                                         │
│  metadata:                            metadata:                         │
│    annotations: {}             ──►      annotations:                    │
│                                           das.openshift.io/mig-profiles:│
│                                             "1g.10gb:1"                 │
│                                                                         │
│  WHAT HAPPENS:                                                          │
│  1. Remove nvidia.com/mig-* from container resources                   │
│  2. Calculate total GPU memory from MIG profiles                       │
│  3. Add gpu.das.openshift.io/mem with total memory                     │
│  4. Store original profiles in annotation (for DAS scheduler)          │
│  5. Set schedulerName to das-scheduler                                 │
│  6. Set runtimeClassName to nvidia-legacy                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Kueue Integration Flow

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      DAS + KUEUE INTEGRATION FLOW                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────┐                                                   │
│   │ User submits    │  Job with:                                        │
│   │ workload        │  • nvidia.com/mig-1g.10gb: 1                      │
│   └────────┬────────┘  • kueue.x-k8s.io/queue-name: my-queue            │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ DAS Webhook     │  Transforms to:                                   │
│   │ (source level)  │  • gpu.das.openshift.io/mem: 10                   │
│   └────────┬────────┘  • das.openshift.io/mig-profiles: "1g.10gb:1"     │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ Kueue           │  Creates Workload CR                              │
│   │ Controller      │  Checks ClusterQueue quota                        │
│   └────────┬────────┘  ADMITS if quota available                        │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ DAS Scheduler   │  1. Filter: nodes with gpu.das.openshift.io/mem   │
│   │                 │  2. Score: prefer bin-packing                     │
│   └────────┬────────┘  3. Read mig-profiles annotation                  │
│            │           4. Create AllocationClaim                        │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ DAS Daemonset   │  1. Watch AllocationClaim                         │
│   │ (on node)       │  2. Execute: nvidia-smi mig -cgi 1g.10gb -C       │
│   └────────┬────────┘  3. Update claim status: Ready                    │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ Pod runs with   │  Has access to dynamically created MIG slice      │
│   │ MIG slice       │                                                   │
│   └─────────────────┘                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Supported Workload Types

### Currently Implemented

| Workload Type | Webhook File | Notes |
|---------------|--------------|-------|
| Pod | `webhook.go` | Direct pod submissions |
| Job | `job_webhook.go` | Kueue-managed Jobs only |
| PyTorchJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| MPIJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| TFJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| XGBoostJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| PaddleJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| JAXJob | `kubeflow_webhook.go` | Kubeflow Training Operator |
| RayJob | `ray_webhook.go` | KubeRay Operator |
| RayCluster | `ray_webhook.go` | KubeRay Operator |
| JobSet | `jobset_webhook.go` | JobSet Controller |
| AppWrapper | `appwrapper_webhook.go` | CodeFlare |
| Deployment | `longrunning_webhook.go` | Long-running services |
| StatefulSet | `longrunning_webhook.go` | Stateful services |
| Workload | `workload_webhook.go` | Kueue Workload CR (safety net) |

---

## Sequence Diagrams

### Simple Job Flow

```
┌──────┐     ┌──────────┐     ┌───────┐     ┌─────────────┐     ┌──────────┐
│ User │     │DAS       │     │ Kueue │     │DAS          │     │DAS       │
│      │     │Webhook   │     │       │     │Scheduler    │     │Daemonset │
└──┬───┘     └────┬─────┘     └───┬───┘     └──────┬──────┘     └────┬─────┘
   │              │               │                │                 │
   │ Create Job   │               │                │                 │
   │ nvidia.com/  │               │                │                 │
   │ mig-1g.10gb  │               │                │                 │
   │──────────────>               │                │                 │
   │              │               │                │                 │
   │              │ Transform:    │                │                 │
   │              │ gpu.das.mem   │                │                 │
   │              │ + annotation  │                │                 │
   │              │───────────────>                │                 │
   │              │               │                │                 │
   │              │               │ Create Workload│                 │
   │              │               │ Check quota    │                 │
   │              │               │ ADMIT          │                 │
   │              │               │────────────────>                 │
   │              │               │                │                 │
   │              │               │                │ Read annotation │
   │              │               │                │ Filter nodes    │
   │              │               │                │ Create AllocClaim
   │              │               │                │─────────────────>
   │              │               │                │                 │
   │              │               │                │                 │ nvidia-smi
   │              │               │                │                 │ mig -cgi
   │              │               │                │                 │ 1g.10gb
   │              │               │                │                 │
   │              │               │                │ Claim Ready     │
   │              │               │                │<─────────────────
   │              │               │                │                 │
   │              │               │                │ Bind Pod        │
   │              │               │<────────────────                 │
   │              │               │                │                 │
   │ Pod Running  │               │                │                 │
   │<──────────────────────────────                │                 │
   │              │               │                │                 │
```

### PyTorchJob with Multiple Workers

```
┌──────┐     ┌──────────┐     ┌───────┐     ┌───────────┐     ┌──────────┐
│ User │     │DAS       │     │ Kueue │     │DAS        │     │DAS       │
│      │     │Kubeflow  │     │       │     │Scheduler  │     │Daemonset │
│      │     │Webhook   │     │       │     │           │     │          │
└──┬───┘     └────┬─────┘     └───┬───┘     └─────┬─────┘     └────┬─────┘
   │              │               │               │                │
   │ PyTorchJob   │               │               │                │
   │ Master: 1x   │               │               │                │
   │   2g.20gb    │               │               │                │
   │ Worker: 2x   │               │               │                │
   │   2g.20gb    │               │               │                │
   │──────────────>               │               │                │
   │              │               │               │                │
   │              │ Transform     │               │                │
   │              │ ALL replicas: │               │                │
   │              │ Master:       │               │                │
   │              │  gpu.mem: 20  │               │                │
   │              │ Worker:       │               │                │
   │              │  gpu.mem: 20  │               │                │
   │              │───────────────>               │                │
   │              │               │               │                │
   │              │               │ Workload:     │                │
   │              │               │ Total: 60GB   │                │
   │              │               │ (20+20+20)    │                │
   │              │               │               │                │
   │              │               │ Quota: 80GB   │                │
   │              │               │ 60 <= 80 ✓    │                │
   │              │               │ ADMIT         │                │
   │              │               │───────────────>                │
   │              │               │               │                │
   │              │               │               │ Schedule       │
   │              │               │               │ 3 pods         │
   │              │               │               │ Create 3       │
   │              │               │               │ AllocationClaims
   │              │               │               │────────────────>
   │              │               │               │                │
   │              │               │               │                │ Create 3x
   │              │               │               │                │ 2g.20gb
   │              │               │               │                │ slices
   │              │               │               │<────────────────
   │              │               │               │                │
   │ PyTorchJob   │               │               │                │
   │ Running      │               │               │                │
   │<──────────────────────────────               │                │
   │              │               │               │                │
```

---

## Known Limitations

### 1. MIG + NCCL Limitation

**CRITICAL: NCCL does not work with MIG instances on the same physical GPU**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     MIG + NCCL LIMITATION                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  NCCL (NVIDIA Collective Communications Library) is used for           │
│  GPU-to-GPU communication in distributed training.                     │
│                                                                         │
│  PROBLEM: NCCL does NOT support multiple ranks per GPU.                │
│           MIG instances on the SAME physical GPU cannot use NCCL.      │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  GPU 0 (80GB A100)                                             │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐                       │    │
│  │  │ Worker-0 │ │ Worker-1 │ │ Worker-2 │                       │    │
│  │  │ 2g.20gb  │ │ 2g.20gb  │ │ 2g.20gb  │                       │    │
│  │  └──────────┘ └──────────┘ └──────────┘                       │    │
│  │       ▲              ▲            ▲                            │    │
│  │       └──────────────┴────────────┘                            │    │
│  │              NCCL FAILS ✗                                      │    │
│  │        (same physical GPU)                                     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌────────────────────┐  ┌────────────────────┐                        │
│  │  GPU 0             │  │  GPU 1             │                        │
│  │  ┌──────────┐      │  │  ┌──────────┐      │                        │
│  │  │ Worker-0 │      │  │  │ Worker-1 │      │                        │
│  │  │ 2g.20gb  │      │  │  │ 2g.20gb  │      │                        │
│  │  └──────────┘      │  │  └──────────┘      │                        │
│  └────────────────────┘  └────────────────────┘                        │
│           ▲                       ▲                                    │
│           └───────────────────────┘                                    │
│                  NCCL WORKS ✓                                          │
│            (different physical GPUs)                                   │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  CURRENT WORKAROUNDS:                                                   │
│                                                                         │
│  1. Use gloo backend instead of NCCL for PyTorch DDP                   │
│     - Works with MIG on same GPU                                       │
│     - Lower performance than NCCL                                      │
│                                                                         │
│  2. Use full GPUs (non-MIG) for NCCL workloads                         │
│     - Request nvidia.com/gpu instead of nvidia.com/mig-*               │
│                                                                         │
│  FUTURE: DAS scheduler could implement GPU-level anti-affinity         │
│          to place NCCL workers on different physical GPUs              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Reference:** [NVIDIA NCCL Issue #431](https://github.com/NVIDIA/nccl/issues/431)

### 2. MIG Profile Constraints

- Not all MIG profiles are available on all GPUs
- A100 40GB vs A100 80GB have different profiles
- Some profile combinations are mutually exclusive

### 3. Workload Requirements

- Workloads must have `kueue.x-k8s.io/queue-name` label to be Kueue-managed
- Non-Kueue workloads follow the original DAS flow (resource renaming)

---

## Implementation Status

### Completed ✓

| Component | Status | Description |
|-----------|--------|-------------|
| Template Mutator | ✓ | `TransformPodTemplateForDAS()` core function |
| Pod Webhook | ✓ | Direct pod mutation |
| Job Webhook | ✓ | Batch Job support |
| Kubeflow Webhooks | ✓ | PyTorchJob, MPIJob, TFJob, XGBoostJob, PaddleJob, JAXJob |
| Ray Webhooks | ✓ | RayJob, RayCluster |
| JobSet Webhook | ✓ | Coordinated job groups |
| AppWrapper Webhook | ✓ | CodeFlare integration |
| Long-running Webhooks | ✓ | Deployment, StatefulSet |
| Workload Webhook | ✓ | Safety net for Kueue Workloads |
| DAS Scheduler | ✓ | MIG profile annotation reading |
| Sample Kueue Setup | ✓ | ResourceFlavor, ClusterQueue, LocalQueue |

### Not Yet Implemented

| Feature | Status | Notes |
|---------|--------|-------|
| NCCL Anti-Affinity | ⚠️ | Requires scheduler changes for GPU-level placement |
| LeaderWorkerSet | ⚠️ | Sample exists, webhook may need testing |
| Gang Scheduling | ⚠️ | PodGroup support for all-or-nothing scheduling |

---

## Setup and Configuration

### Prerequisites

1. **Kueue installed** - `kubectl get pods -n kueue-system`
2. **DAS operator installed** - All four components running
3. **MIG-capable GPUs** - A100, H100, or A30

### Kueue Configuration

Apply the sample setup:

```bash
kubectl apply -f samples/kueue/00-kueue-setup.yaml
```

This creates:

```yaml
# ResourceFlavor - represents GPU resources
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: das-gpu-flavor
spec:
  nodeLabels: {}

---
# ClusterQueue - defines quota
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: das-cluster-queue
spec:
  namespaceSelector: {}
  resourceGroups:
  - coveredResources:
    - gpu.das.openshift.io/mem  # DAS GPU memory resource
    - cpu
    - memory
    flavors:
    - name: das-gpu-flavor
      resources:
      - name: gpu.das.openshift.io/mem
        nominalQuota: "80"  # 80GB total GPU memory
      - name: cpu
        nominalQuota: "100"
      - name: memory
        nominalQuota: "200Gi"

---
# LocalQueue - namespace-scoped queue
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: das-local-queue
  namespace: default
spec:
  clusterQueue: das-cluster-queue
```

### Example Workload

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: mig-job
  labels:
    kueue.x-k8s.io/queue-name: das-local-queue  # Required for Kueue
spec:
  template:
    spec:
      containers:
      - name: cuda
        image: nvidia/cuda:12.0-base
        command: ["nvidia-smi", "-L"]
        resources:
          limits:
            nvidia.com/mig-1g.10gb: 1  # DAS transforms this
      restartPolicy: Never
```

### MIG Profile to Memory Mapping

| MIG Profile | GPU Memory | gpu.das.openshift.io/mem |
|-------------|------------|--------------------------|
| 1g.5gb | 5 GB | 5 |
| 1g.10gb | 10 GB | 10 |
| 2g.10gb | 10 GB | 10 |
| 2g.20gb | 20 GB | 20 |
| 3g.20gb | 20 GB | 20 |
| 3g.40gb | 40 GB | 40 |
| 4g.40gb | 40 GB | 40 |
| 7g.80gb | 80 GB | 80 |

---

## Related Resources

- [Kueue Documentation](https://kueue.sigs.k8s.io/)
- [NVIDIA MIG User Guide](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/)
- [Kubeflow Training Operator](https://github.com/kubeflow/training-operator)
- [KubeRay](https://github.com/ray-project/kuberay)
