## 1. How KServe handles multi-node and disaggregated inference today

KServe exposes a CRD called `LLMInferenceService` (LLMISVC). Its reconciler decides whether a model is a single-node, multi-node, or disaggregated (P/D) workload based on the spec, and then delegates the actual pod orchestration to **LeaderWorkerSet (LWS)** when the deployment is multi-node.

The entry point lives in `kserve/pkg/controller/v1alpha2/llmisvc/workload.go`:

```57:118:kserve/pkg/controller/v1alpha2/llmisvc/workload.go
func (r *LLMISVCReconciler) reconcileWorkload(ctx context.Context, llmSvc *v1alpha2.LLMInferenceService, config *Config) error {
	logger := log.FromContext(ctx).WithName("reconcileWorkload")
	ctx = log.IntoContext(ctx, logger)

	logger.Info("Reconciling Workload")
	...
	// Handle multi-node deployments using LeaderWorkerSets
	if err := r.reconcileMultiNodeWorkload(ctx, llmSvc, config); err != nil {
		llmSvc.MarkWorkerWorkloadNotReady("ReconcileMultiNodeWorkloadError", err.Error())
		return fmt.Errorf("failed to reconcile multi node workload: %w", err)
	}

	// Handle single-node deployments using standard Deployments
	if err := r.reconcileSingleNodeWorkload(ctx, llmSvc, config); err != nil {
		llmSvc.MarkMainWorkloadNotReady("ReconcileSingleNodeWorkloadError", err.Error())
		return fmt.Errorf("failed to reconcile single node workload: %w", err)
	}
	...
}
```

For multi-node and disaggregated cases, KServe spawns **two independent LWS objects**: one for the "main" workload (decode, or `both`) and one for prefill:

```41:57:kserve/pkg/controller/v1alpha2/llmisvc/workload_multi_node.go
func (r *LLMISVCReconciler) reconcileMultiNodeWorkload(ctx context.Context, llmSvc *v1alpha2.LLMInferenceService, config *Config) error {
	log.FromContext(ctx).Info("Reconciling multi-node workload")

	if err := r.reconcileMultiNodeMainServiceAccount(ctx, llmSvc, config); err != nil {
		return fmt.Errorf("failed to reconcile multi-node service account: %w", err)
	}
	if err := r.reconcileMultiNodePrefillServiceAccount(ctx, llmSvc); err != nil {
		return fmt.Errorf("failed to reconcile multi-node service account: %w", err)
	}
	if err := r.reconcileMultiNodeMainWorkload(ctx, llmSvc, config); err != nil {
		return fmt.Errorf("failed to reconcile multi-node main workload: %w", err)
	}
	if err := r.reconcileMultiNodePrefillWorkload(ctx, llmSvc, config); err != nil {
		return fmt.Errorf("failed to reconcile multi-node prefill workload: %w", err)
	}
	return nil
}
```

The "main" LWS uses the rolling-update strategy and `LeaderCreatedStartupPolicy`:

```157:182:kserve/pkg/controller/v1alpha2/llmisvc/workload_multi_node.go
	expected := &lwsapi.LeaderWorkerSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mainLWSName(llmSvc),
			Namespace: llmSvc.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(llmSvc, v1alpha2.LLMInferenceServiceGVK),
			},
			Labels: workerLabels,
		},
		Spec: lwsapi.LeaderWorkerSetSpec{
			Replicas: llmSvc.Spec.Replicas,
			LeaderWorkerTemplate: lwsapi.LeaderWorkerTemplate{
				Size: llmSvc.Spec.Parallelism.GetSize(),
				WorkerTemplate: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: workerLabels,
					},
				},
				RestartPolicy: lwsapi.RecreateGroupOnPodRestart,
			},
			RolloutStrategy: lwsapi.RolloutStrategy{
				Type: lwsapi.RollingUpdateStrategyType,
			},
			StartupPolicy: lwsapi.LeaderCreatedStartupPolicy,
		},
	}
```

Pods carry an `llm-d.ai/role` label that tells the `llm-d` gateway/EPP which role they play (prefill, decode, or both):

```411:417:kserve/pkg/constants/constants.go
// LLM-d role label (key and values for "llm-d.ai/role" on workload pods)
const (
	LLMDRoleLabelKey = "llm-d.ai/role"
	LLMDRoleDecode   = "decode"
	LLMDRolePrefill  = "prefill"
	LLMDRoleBoth     = "both"
)
```

The data plane is handled by an **`llm-d` routing sidecar** injected into the worker/leader pod spec, and the **`llm-d` EPP/Inference Scheduler** sitting in front of the Inference Gateway. KServe simply detects whether the sidecar is present:

```40:53:kserve/pkg/controller/v1alpha2/llmisvc/workload.go
const (
	// routingSidecarContainerName is the name of the routing sidecar container
	// that handles prefill disaggregation routing.
	routingSidecarContainerName = "llm-d-routing-sidecar"

	defaultServiceAccountName = "default"
)

// sidecarSSRFProtectionRules defines RBAC rules for the routing sidecar
// These permissions are needed to discover and monitor inference pools and pods.
var sidecarSSRFProtectionRules = []rbacv1.PolicyRule{
	{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{"inference.networking.x-k8s.io"}, Resources: []string{"inferencepools"}, Verbs: []string{"get", "list", "watch"}},
}
```

Routing flow at request time (from `llm-d/docs/architecture/advanced/disaggregation/README.md`):

> Client → Gateway/Proxy → EPP runs the `disagg-profile-handler` → EPP picks a P endpoint and a D endpoint → request hits the routing-sidecar inside the Decode pod → sidecar issues `do_remote_decode=True` to Prefill with `max_tokens=1` → Prefill returns KV transfer params → sidecar forwards the decode request → Decode pulls the KV cache from Prefill via **NIXL** (RDMA when available, TCP otherwise).

Autoscaling for both prefill and decode is delegated to the **`llm-d` Workload Variant Autoscaler (WVA)** plus an HPA or KEDA ScaledObject. The scale targets are the two independent LWS objects:

```443:471:kserve/pkg/controller/v1alpha2/llmisvc/scaling.go
func mainScaleTargetRef(llmSvc *v1alpha2.LLMInferenceService) autoscalingv2.CrossVersionObjectReference {
	if llmSvc.Spec.Worker != nil {
		return autoscalingv2.CrossVersionObjectReference{
			APIVersion: lwsapi.GroupVersion.String(),
			Kind:       "LeaderWorkerSet",
			Name:       mainLWSName(llmSvc),
		}
	}
	...
}

func prefillScaleTargetRef(llmSvc *v1alpha2.LLMInferenceService) autoscalingv2.CrossVersionObjectReference {
	if llmSvc.Spec.Prefill != nil && llmSvc.Spec.Prefill.Worker != nil {
		return autoscalingv2.CrossVersionObjectReference{
			APIVersion: lwsapi.GroupVersion.String(),
			Kind:       "LeaderWorkerSet",
			Name:       prefillLWSName(llmSvc),
		}
	}
	...
}
```

So the current KServe stack maps cleanly to:

| Concern                  | What KServe uses today                                |
| ------------------------ | ----------------------------------------------------- |
| Pod orchestration        | `LeaderWorkerSet` (one per role, prefill and main)    |
| Gang scheduling          | Whatever LWS itself does (per-LWS only)               |
| Service / routing        | Single ClusterIP service + `llm-d` EPP / Gateway      |
| KV-aware routing         | `llm-d` EPP `disagg-profile-handler`                  |
| KV transfer              | `llm-d` routing sidecar + NIXL                        |
| Autoscaling              | `llm-d-workload-variant-autoscaler` + HPA / KEDA      |
| Topology hints           | Propagation of `kueue.x-k8s.io/*` annotations to LWS  |
| Multiple backends        | vLLM (primary), SGLang via sidecar protocol           |

---

## 2. Flaws in the current KServe approach

Evaluating KServe (LWS + `llm-d`) against the five pillars Grove explicitly targets:

### 2.1 Hierarchical Gang Scheduling — ❌
KServe creates **two unrelated** LWS objects (`expectedMainMultiNodeLWS`, `expectedPrefillMultiNodeLWS`). Each LWS gang-schedules **its own** pods (leader + workers of a single replica), but there is no scheduler-level guarantee that "at least one prefill replica AND at least one decode replica must be schedulable together."

In a tight cluster, the scheduler can happily place all Prefill pods, then leave Decode `Pending`. The pipeline is functionally broken even though everything looks fine to KServe and LWS individually.

### 2.2 Startup Ordering — ❌
KServe sets `StartupPolicy: lwsapi.LeaderCreatedStartupPolicy` on each LWS, but this only governs leader-before-worker ordering **inside a single LWS**. There is nothing that says "Decode must wait for Prefill to be Ready." Pods come up in whatever order the scheduler binds them, relying on retries/CrashLoopBackoff and the routing sidecar to eventually reach steady state.

### 2.3 Rolling Updates — 🟨 (works, but unsafe for P/D)
Each LWS rolls itself using its own RollingUpdate strategy. The two LWS objects roll **independently**. Because the same `Service` selector targets pods purely by `llm-d.ai/role` (no revision filter), the EPP can pair a `v2` Prefill with a `v1` Decode mid-rollout. The KV transfer protocol uses the model's internal tensor/cache layout; if the model code or KV format shifts between versions, you get failed transfers or 500s during the rollout window. This is the "split-brain" problem.

### 2.4 Topology-Aware Placement — 🟨 (only via annotation passthrough)
KServe forwards Kueue-related labels/annotations to the generated LWS:

```600:613:kserve/pkg/controller/v1alpha2/llmisvc/workload_multi_node.go
func (r *LLMISVCReconciler) propagateLeaderWorkerSetMetadata(llmSvc *v1alpha2.LLMInferenceService, expected *lwsapi.LeaderWorkerSet) {
	// Define the prefixes to approve for annotations and labels
	approvedAnnotationPrefixes := []string{
		"leaderworkerset.sigs.k8s.io",
		"k8s.v1.cni.cncf.io",
		constants.KueueAPIGroupName,
		"prometheus.io",
		constants.LocalModelLabel,
	}
	approvedLabelPrefixes := []string{
		constants.KueueAPIGroupName,
		constants.LocalModelLabel,
	}
```

This lets users opt into LWS's "exclusive-topology" feature or Kueue queues. But that is **coarse**: at best it can co-locate a single LWS's pods inside one "topology block." It does not understand NVLink-domain vs rack vs zone hierarchies, cannot force the prefill PCSG and decode PCSG into the same block, and provides no scheduler-level fallback semantics.

### 2.5 Multi-Level Horizontal Auto-Scaling — ✅
This one actually works. WVA + HPA (or KEDA) target each LWS independently. Prefill and Decode can scale asymmetrically. This is the area where KServe is closest to parity with Grove.

### 2.6 Backend support
Only vLLM is supported as a first-class backend today (`huggingfaceserver` + presets are vLLM-centric). SGLang is reachable through the routing sidecar's `sglang` protocol, but the KServe presets in `config/presets/llm-d/` are vLLM-oriented. TensorRT-LLM is not natively supported.

---

## 3. The Grove model

Grove (NVIDIA Dynamo) builds an inference workload out of four layered concepts (defined in `grove/README.md` and the operator API):

- **`PodClique`** — a group of pods that all play the same role (e.g., a "prefill leader" clique, a "decode worker" clique). Each clique carries its own pod template, replica count, and optional `AutoScalingConfig`.
- **`PodCliqueScalingGroup` (PCSG)** — a bundle of PodCliques that must scale and schedule together as one gang. The natural use case is "prefill leader + prefill worker" or "decode leader + decode worker" — roles that are tightly coupled.
- **`PodCliqueSet` (PCS)** — the top-level user-facing object. It owns a set of PodCliques and PodCliqueScalingGroups that make up one complete inference pipeline (e.g., the full prefill + decode + router graph). It also supports replicas of itself for HA.
- **`PodGang`** — the scheduler-facing object the operator emits from a PCS. It carries the gang-scheduling contract (`MinReplicas` per group) and the topology constraints. A custom scheduler backend (Kai, Yunikorn, etc.) admits or rejects the whole `PodGang` atomically.

A complete multi-node disaggregated inference graph fits in **a single PodCliqueSet**:

```12:103:grove/operator/samples/user-guide/01_core-concepts/multi-node-disaggregated.yaml
apiVersion: grove.io/v1alpha1
kind: PodCliqueSet
metadata:
  name: multinode-disaggregated
  namespace: default
spec:
  replicas: 1
  template:
    cliques:
    - name: pleader
      spec:
        roleName: pleader
        replicas: 1
        podSpec: ...
    - name: pworker
      spec:
        roleName: pworker
        replicas: 4
        podSpec: ...
    - name: dleader
      spec:
        roleName: dleader
        replicas: 1
        podSpec: ...
    - name: dworker
      spec:
        roleName: dworker
        replicas: 2
        podSpec: ...
    podCliqueScalingGroups:
    - name: prefill
      cliqueNames: [pleader, pworker]
      replicas: 2
    - name: decode
      cliqueNames: [dleader, dworker]
      replicas: 1
```

Grove explicitly claims all five pillars as already-shipped (`grove/README.md`):

```70:77:grove/README.md
### 2025 Priorities

- Hierarchical Gang Scheduling ✅
- Multi-Level Horizontal Auto-Scaling ✅
- Startup Ordering ✅
- Rolling Updates ✅
- Topology-Aware Scheduling ✅
```

Let's look at how these are actually implemented.

### 3.1 Hierarchical gang scheduling via `PodGang`
The operator translates a PodCliqueSet into one or more `PodGang` scheduler objects. `PodGang` carries `MinReplicas` per pod group plus a top-level `TopologyConstraint`, and the custom scheduler backend (Kai, Yunikorn, etc.) treats the whole gang atomically.

`PodGangPhase` defines the state machine: `Pending → Starting → Running`, with conditions like `PodGangConditionTypeScheduled` and `PodGangConditionTypeReady`. If the gang cannot be fully placed, the constituent pods stay `Pending` — the scheduler never partially admits.

### 3.2 Startup ordering — enforced by an init container
This is the most concrete piece of code in Grove. When a `PodClique` defines `StartsAfter`, the operator mutates the pod to inject a Grove init container:

```50:55:grove/operator/internal/controller/podclique/components/pod/initcontainer.go
// configurePodInitContainer adds the necessary volumes and init container to the pod for dependency management
func configurePodInitContainer(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, pod *corev1.Pod) error {
	addServiceAccountTokenSecretVolume(pcs.Name, pod)
	addPodInfoVolume(pod)
	return addInitContainer(pcs, pclq, pod)
}
```

The init container is `grove-initc`:

```36:48:grove/operator/internal/controller/podclique/components/pod/initcontainer.go
const (
	// envVarInitContainerImage stores the environment variable which is read to find the image for the init-container.
	// The environment variable should only store the registry and repository of the init-container. It should not contain any tag.
	envVarInitContainerImage string = "GROVE_INIT_CONTAINER_IMAGE"
	// initContainerName is the name of the init container.
	initContainerName = "grove-initc"
	// serviceAccountTokenSecretVolumeName is the name of the volume that mounts the service account token secret.
	serviceAccountTokenSecretVolumeName = "sa-token-secret-vol"
	// podInfoVolumeName is the name of the downwardAPI volume that passes the pod information to the init container.
	podInfoVolumeName = "pod-info-vol"
	// volumeMountPathServiceAccount is the base path where token and CA.cert for the service account will be placed.
	volumeMountPathServiceAccount = "/var/run/secrets/kubernetes.io/serviceaccount"
)
```

Inside the init container, it blocks on the API server until every parent PodClique has at least `MinAvailable` pods Ready:

```35:63:grove/operator/initc/cmd/main.go
func main() {
	ctx := setupSignalHandler()

	config, err := opts.InitializeCLIOptions()
	if err != nil {
		log.Error(err, "Failed to generate configuration for the init container from the flags")
		os.Exit(1)
	}

	log.Info("Starting grove init container")

	podCliqueDependencies, err := config.GetPodCliqueDependencies()
	if err != nil {
		log.Error(err, "Failed to parse CLI input")
		os.Exit(1)
	}

	podCliqueState, err := internal.NewPodCliqueState(podCliqueDependencies, log)
	if err != nil {
		os.Exit(1)
	}

	if err = podCliqueState.WaitForReady(ctx, log); err != nil {
		log.Error(err, "Failed to wait for all parent PodCliques")
		os.Exit(1)
	}
```

Startup type can be `AnyOrder` (default), `InOrder` (linear order of cliques), or `Explicit` (via `StartsAfter`):

```440:448:grove/operator/api/core/v1alpha1/podcliqueset.go
const (
	// CliqueStartupTypeAnyOrder defines that the cliques can be started in any order. This allows for concurrent starts of cliques.
	// This is the default CliqueStartupType.
	CliqueStartupTypeAnyOrder CliqueStartupType = "CliqueStartupTypeAnyOrder"
	// CliqueStartupTypeInOrder defines that the cliques should be started in the order they are defined in the PodGang Cliques slice.
	CliqueStartupTypeInOrder CliqueStartupType = "CliqueStartupTypeInOrder"
	// CliqueStartupTypeExplicit defines that the cliques should be started after the cliques defined in PodClique.StartsAfter have started.
	CliqueStartupTypeExplicit CliqueStartupType = "CliqueStartupTypeExplicit"
)
```

### 3.3 Rolling updates — `RollingRecreate` (default) and `OnDelete`
Grove's default strategy is `RollingRecreate`: it deletes a replica (e.g., a single PCSG replica or a standalone PCLQ pod), waits for capacity, recreates it. Updates are orchestrated **at the replica/clique level**, not blindly across the cluster:

```39:72:grove/operator/internal/controller/podcliqueset/components/podcliquesetreplica/rollingupdate.go
func (r _resource) orchestrateRollingUpdate(ctx context.Context, logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet, pcsIndicesToTerminate, minAvailableBreachedPCSReplicaIndices []int) error {
	updateWork, err := r.computePendingUpdateWork(ctx, pcs, pcsIndicesToTerminate)
	if err != nil {
		return err
	}

	if len(pcs.Status.UpdateProgress.CurrentlyUpdating) > 0 && updateWork.currentlyUpdatingReplicaInfo != nil {
		if err = r.updatePCSWithReplicaUpdateProgress(ctx, logger, pcs, updateWork.currentlyUpdatingReplicaInfo.updateProgress); err != nil {
			return err
		}
		if !updateWork.currentlyUpdatingReplicaInfo.updateProgress.done {
			return groveerr.New(
				groveerr.ErrCodeContinueReconcileAndRequeue,
				component.OperationSync,
				fmt.Sprintf("rolling update of PodCliqueSet replica index %d is not completed", updateWork.currentlyUpdatingReplicaInfo.replicaIndex),
			)
		}
	}

	// pick the next replica index to update.
	nextReplicaToUpdate := updateWork.getNextReplicaToUpdate(pcs, minAvailableBreachedPCSReplicaIndices)
	...
}
```

`OnDelete` (proposed in `docs/proposals/291-ondelete-update/README.md`) leaves replicas alone until manually deleted, which is invaluable when you don't want template changes to disrupt long-running, expensive inference replicas.

#### LWS vs. Grove rolling updates — at a glance

LWS and Grove make different trade-offs. The summary:

- **Unit of update.** LWS rolls **one LWS group** (leader + workers) at a time. Grove rolls **one pod / PCSG replica / PCS replica** depending on which layer changed.
- **Surge vs. recreate.** LWS defaults to **surge** (spin up new alongside old, then tear down old) — requires spare GPU capacity. Grove defaults to **`RollingRecreate`** (delete old, then create new in its place) — needs no spare GPUs, accepts brief per-replica downtime.
- **Manual control.** LWS has no opt-out from automatic rollouts. Grove offers `OnDelete`, where template changes do not auto-trigger restarts; pods only adopt the new template when a human deletes them.
- **Cross-role coordination.** LWS coordinates only within one LWS — two LWS objects (Prefill and Decode in KServe today) can drift out of sync during a rollout. Grove coordinates across the entire `PodCliqueSet`, so prefill and decode in the same PCS replica always end up on the same template before traffic flows to it.
- **Stuck-replica behavior.** LWS keeps retrying the failing group while the rest of the workload continues on the old template. Grove blocks the rollout entirely until the in-flight replica reaches Ready (`"rolling update of currently selected PCSG replica index N is not complete, requeuing"`), preventing cascading failures.
- **GPU-economy preference.** LWS trade: cluster needs spare capacity. Grove trade: each replica is briefly down during its own recreate cycle, but the rest stay serving.

`DisaggregatedSet` introduces a third model layered on top of LWS: an **N-dimensional linear-interpolation planner** that coordinates two or more LWS objects (e.g., prefill + decode) so they roll out in lockstep, plus revision-isolated headless Services so the gateway never pairs `v1` prefill with `v2` decode. This is the cheapest way to retrofit Grove-style coordination into the LWS world without rewriting the scheduler.

### 3.4 Topology-aware placement — `ClusterTopology` + `TopologyConstraint`
Cluster admins declare the data center's physical hierarchy as a `ClusterTopology` CR, listing levels (zone → block → rack → host → numa) and the node-label key for each level. Workloads request a `TopologyConstraint`:

```264:279:grove/operator/api/core/v1alpha1/podcliqueset.go
// TopologyConstraint defines topology placement requirements.
type TopologyConstraint struct {
	// TopologyName is the name of the ClusterTopology resource to use for topology-aware scheduling.
	// If topologyConstraint is set, topologyName and packDomain must both be specified.
	// Immutable after creation.
	// +required
	TopologyName string `json:"topologyName"`
	// PackDomain specifies the topology domain for grouping replicas.
	// Controls placement constraint for EACH individual replica instance.
	// Must reference a domain in the topology levels defined in the ClusterTopology CR name as set in TopologyName
	// Example: "rack" means each replica independently placed within one rack.
	// Note: Does NOT constrain all replicas to the same rack together.
	// Different replicas can be in different topology domains.
	// +required
	PackDomain TopologyDomain `json:"packDomain"`
}
```

`TopologyConstraint`s can be specified at PodCliqueSet, PodCliqueScalingGroup, and PodClique levels. The validating webhook enforces hierarchical strictness (child domain ≤ parent), single-topology-per-PCS (Phase 1 restriction), and existence of the referenced `ClusterTopology`.

The operator then translates `packDomain` into a `TopologyPackConstraint` on the underlying `PodGang`:

```99:117:grove/scheduler/api/core/v1alpha1/podgang.go
// TopologyPackConstraint defines a topology packing constraint.
// Each of Required and Preferred fields hold a topologyKey, e.g. "kubernetes.io/hostname" ( these are key of labels added on nodes).
type TopologyPackConstraint struct {
	// Required defines a topology constraint that must be satisfied as a hard requirement. The workload will not be
	// scheduled if this constraint cannot be satisfied. Generally, it is easier for the scheduler to satisfy constraints
	// on topology domains with larger compute capacity, (e.g. zone or datacenter), than smaller domains, (e.g. host or
	// numa). Holds topologyKey (not level name) translated from user's packLevel specification.
	// Example: "topology.kubernetes.io/rack"
	// +optional
	Required *string `json:"required,omitempty"`
	// Preferred defines best-effort topology constraint. Topology domains that provide the most optimized performance
	// with dense packing (e.g. host or numa) are typically used as preferred constraints for topology packing. It might be
	// harder to satisfy these constraints if the topology domains are limited in compute capacity. Since it is preferred
	// constraint, it is therefore not binding on the scheduler to mandatorily satisfy this packing constraint. Scheduler
	// can fall back to higher topology levels (upto Required constraint) if preferred cannot be satisfied.
	// Example: "kubernetes.io/hostname"
	// +optional
	Preferred *string `json:"preferred,omitempty"`
}
```

Important nuance from the API design: `Required` causes the scheduler to **refuse to admit** the gang if it cannot pack within the required domain. `Preferred` is best-effort. This matters for the question "what if no NVLink domain is free?" — that depends on whether the user set the constraint as `Required` or `Preferred`. So Grove gives the *operator* the choice between strict isolation and graceful fallback; it does not force one over the other.

### 3.5 Multi-level autoscaling
Each PodClique can carry its own `AutoScalingConfig`. Each PodCliqueScalingGroup can also carry a `ScaleConfig`. The PodCliqueSet itself can scale via top-level replicas. This gives three independent levels of scaling: a single role (clique), a coupled-role gang (scaling group), and the whole pipeline (set). KServe today has only one level of asymmetry: per-LWS via the WVA.

---

## 4. Where does `DisaggregatedSet` fit, and what does it solve?

`DisaggregatedSet` is a new CRD developed under the LWS (`kubernetes-sigs/lws`) project and lives in `lws/disaggregatedset/`. It is **not yet integrated into KServe** — KServe still generates raw LWS objects (see `workload_multi_node.go`). KEP-766 in `lws/keps/766-DisaggregatedSet/README.md` describes the design.

### 4.1 What it is
`DisaggregatedSet` is a higher-level controller that **owns multiple `LeaderWorkerSet` objects** as roles within one disaggregated workload. From `lws/disaggregatedset/README.md`:

```9:16:lws/disaggregatedset/README.md
DisaggregatedSet simplifies deploying these workloads by:

- **Unified Management**: Manage multiple roles (prefill, decode, etc.) as a single resource
- **Coordinated Rolling Updates**: N-dimensional rollout algorithm updates all roles in lockstep
- **Automatic Service Discovery**: Headless service auto-created for each role
- **Flexible Role Configuration**: Support for 2-10 roles with arbitrary names
- **Stateless Operator**: Safe to restart at any point during operations
```

A `DisaggregatedSet` declares roles, each carrying a `LeaderWorkerTemplateSpec`. The controller hashes all role templates together into a **revision**, then creates per-role LWS objects named `{name}-{revision}-{role}` and per-role headless Services named `{name}-{role}-{revision}-prv` (see `service_manager.go`).

### 4.2 The N-dimensional rolling update algorithm
The interesting bit is `lws/disaggregatedset/internal/controller/planner.go`. It treats each role as a dimension and walks the rollout via linear interpolation, with three guarantees:

1. **Ratio preservation** — if you run 5P2D, the rollout keeps roughly a 5:2 ratio between old/new pods through every intermediate step.
2. **Scale-up before scale-down** — new replicas come up first; old replicas drain only when surge allows.
3. **Coordinated drain** — if any role reaches 0, all roles are forced to 0 (no orphan capacity).

Core math:

```92:131:lws/disaggregatedset/internal/controller/planner.go
// computeNextNewReplicas computes the next new replica count for scale-up.
//
// Linear interpolation: newAtStep(i) = ceil(i * target / totalSteps)
//
// Uses min step index across dimensions to keep roles in sync.
func computeNextNewReplicas(target, currentNew RoleReplicaState, totalSteps int) RoleReplicaState {
	numRoles := len(target)
	if totalSteps == 0 {
		result := make([]int, numRoles)
		copy(result, target)
		return result
	}

	// Step 1: figure out which step we're at based on current replicas
	stepIndex := func(current, targetVal int) int {
		if targetVal == 0 {
			return totalSteps
		}
		return int(float64(current) * float64(totalSteps) / float64(targetVal))
	}

	minStepIdx := totalSteps
	for i := 0; i < numRoles; i++ {
		stepIdx := stepIndex(currentNew[i], target[i])
		minStepIdx = min(minStepIdx, stepIdx)
	}
	nextStepIdx := minStepIdx + 1
	...
}
```

```133:178:lws/disaggregatedset/internal/controller/planner.go
// computeNextOldReplicas computes the next old replica count for scale-down.
//
// Linear interpolation: oldAtStep(i) = source - floor(i * source / totalSteps)
//
// Uses max step index across dimensions to ensure all roles drain together.
func computeNextOldReplicas(source, currentOld RoleReplicaState, totalSteps int) RoleReplicaState {
	...
}
```

Each reconciliation produces an `UpdateStep` that touches either old OR new replicas (decoupled). The controller is stateless — it derives the current step from observed replicas — so restarting it mid-rollout is safe.

### 4.3 Revision-isolated networking
Service creation uses the revision in both the Service name and the selector, so different revisions get different routing pools:

```120:178:lws/disaggregatedset/internal/controller/service_manager.go
// ensureService creates a headless portless Service for a specific role and revision.
func (manager *ServiceManager) ensureService(
	ctx context.Context,
	deployment *disaggv1alpha1.DisaggregatedSet,
	roleName string,
	revision string,
) error {
	...
	service := manager.buildService(deployment, roleName, revision)
	...
}

// buildService constructs a headless portless Service object for a specific role and revision.
func (manager *ServiceManager) buildService(
	deployment *disaggv1alpha1.DisaggregatedSet,
	roleName string,
	revision string,
) *corev1.Service {
	serviceName := GenerateServiceName(deployment.Name, roleName, revision)

	// Standard labels only - no user configuration
	labels := map[string]string{
		LabelDisaggName: deployment.Name,
		LabelDisaggRole: roleName,
		LabelRevision:   revision,
	}

	// Selector matches pod labels for this role and revision
	selector := map[string]string{
		LabelDisaggName: deployment.Name,
		LabelDisaggRole: roleName,
		LabelRevision:   revision,
	}
	...
}
```

This is the structural fix for the "split-brain" rollout problem from §2.3: `DisaggregatedSet`'s strict service isolation allows orchestrators (like KServe) to create a separate Gateway API `InferencePool` for each revision pointing to these specific services. Using `HTTPRoute` weights, KServe can split traffic. When the `llm-d` EPP receives a request for the `v2` pool, it *only* sees `v2` Prefill and Decode endpoints, naturally pairing them. This requires zero code changes to `llm-d` itself.

### 4.4 What `DisaggregatedSet` does NOT solve
Going back to the Grove pillars, here is how `DisaggregatedSet` measures up:

- **Coordinated rolling updates:** ✅ Uses an N-dimensional planner to keep roles in sync during updates.
- **Revision-isolated routing:** ✅ Creates per-revision headless services to prevent "split-brain" routing between old and new pods.
- **Stateless / safe restart:** ✅ The controller derives its state from the cluster, making it safe to restart at any time.
- **Hierarchical gang scheduling:** ❌ Still relies on per-LWS gang scheduling. It cannot guarantee that the entire pipeline (Prefill + Decode) is admitted as a single atomic unit.
- **Startup ordering across roles:** ❌ Has no equivalent to Grove's `StartsAfter` init-container. Roles start concurrently.
- **Topology awareness:** ❌ Relies on passing Kueue labels at best. It has no first-class API for declaring hardware topology boundaries.
- **Multi-level autoscaling:** ❌ Out of scope (KEP §Non-Goals).



So it solves the **lifecycle/rollout half** of the problem set (the things that don't require a custom scheduler) and leaves the **scheduler/placement half** to Grove or other downstream tooling.

---

## 5. Could we integrate `DisaggregatedSet` into KServe?

Yes, and it does **not** create a "two-controller" problem if integrated correctly, because Kubernetes already uses `OwnerReferences` to express hierarchy.

### 5.1 The integration shape
KServe's LLMISVC reconciler would, in the multi-node + disaggregated case, emit **one `DisaggregatedSet`** instead of two raw LWS objects. The `DisaggregatedSet` would have roles `prefill` and `decode` (or `main`). KServe sets itself as the controller of the `DisaggregatedSet` via `OwnerReferences`, and `DisaggregatedSet` becomes the controller of its child LWS objects.

The replace path is small in scope:

- `reconcileMultiNodeMainWorkload` and `reconcileMultiNodePrefillWorkload` collapse into one `reconcileMultiNodeDisaggSet`.
- `expectedMainMultiNodeLWS` and `expectedPrefillMultiNodeLWS` become `expectedDisaggregatedSet`.
- Status propagation (`propagateLeaderWorkerSetStatus`) reads from `DisaggregatedSet.status` instead of two LWS conditions.

### 5.2 Why this is not a "two-controller" problem
A "two-controller" problem only happens when two controllers contend for the **same field** on the **same resource**. The fix is ownership.

In this integration:

- KServe owns the **`DisaggregatedSet`**. It writes `spec.roles[*]` (replicas, templates, etc.).
- The DisaggregatedSet controller owns the **child LWS objects**. It is the only writer of those LWS specs.
- The LWS controller owns the **pods**.

KServe never touches the child LWS directly. The LWS replica count on the child is governed by either `DisaggregatedSet`'s rollout planner (during updates) or `PreserveLWSReplicas()` semantics (so HPA / WVA can still mutate it). The contract is the same as Deployment → ReplicaSet → Pod in stock Kubernetes.

### 5.3 What KServe gains immediately
1. Safe rollouts when both Prefill and Decode change in the same `LLMInferenceService.spec` update.
2. Revision-isolated headless services that the `llm-d` EPP can use to filter out cross-revision endpoint pairs.
3. A "coordinated drain" safety net: if a Prefill misconfiguration crash-loops, Decode is not left holding GPUs.

### 5.4 What KServe does NOT gain
- Gang scheduling across roles (still pending; would need a scheduler-aware solution like Grove or Kueue with TopologyAwareScheduling).
- Hardware topology placement (NVLink/RDMA domain enforcement).
- Cross-role startup ordering.
- Backend pluggability beyond what LWS already supports.

This is why the KEP-766 design is described as a "Phase 1 / shallow" integration: it fixes the lifecycle layer but leaves the scheduler layer untouched.

---

## 6. Grove's topology model — why standard KServe cannot match it today

Grove's topology story is layered:

1. **Cluster admin** declares physical reality once via `ClusterTopology` (`grove/operator/api/core/v1alpha1/clustertopology.go`). It lists topology levels (e.g., `zone`, `block`, `rack`, `host`, `numa`), the node label key for each level, and which scheduler backends to use.
2. **Workload author** sets `TopologyConstraint{TopologyName, PackDomain}` at the PCS / PCSG / PCLQ level. The webhook enforces:
    - All referenced domains exist in `ClusterTopology`.
    - Child constraints are equal-or-narrower than parent (no broadening).
    - All `topologyName` references within a single PCS match (Phase 1).
    - `topologyName` is immutable.
3. **Grove operator** translates `packDomain` into a concrete node-label key, embeds it as a `TopologyPackConstraint` in the resulting `PodGang`.
4. **Scheduler backend** (Kai-scheduler, Yunikorn, etc.) reads the `PodGang` and respects `Required` / `Preferred` packing. If `Required` cannot be satisfied, the gang is not admitted.

This stack gives **all the leverage points an AI infra team needs**:

- Cluster admin controls the hierarchy keys (Grove does not assume specific keys).
- Workload author chooses where to pack (host, rack, block...).
- Whether to fall back is explicit per-constraint (`Required` vs `Preferred`).
- The scheduler does the actual work.

### Why KServe (today) cannot match this

KServe passes Kueue annotations through:

```600:613:kserve/pkg/controller/v1alpha2/llmisvc/workload_multi_node.go
func (r *LLMISVCReconciler) propagateLeaderWorkerSetMetadata(llmSvc *v1alpha2.LLMInferenceService, expected *lwsapi.LeaderWorkerSet) {
	// Define the prefixes to approve for annotations and labels
	approvedAnnotationPrefixes := []string{
		"leaderworkerset.sigs.k8s.io",
		"k8s.v1.cni.cncf.io",
		constants.KueueAPIGroupName,
		"prometheus.io",
		constants.LocalModelLabel,
	}
```

This gives you Kueue-style "topology-aware scheduling" features when LWS is paired with Kueue. But:

- Kueue's hierarchy is independent of any scheduler-level gang semantics. Multiple LWS cannot be co-packed atomically.
- There is no equivalent to a single `PodGang` spanning both Prefill and Decode that gets admitted or rejected as a whole.
- KServe's spec has no first-class field equivalent to `TopologyConstraint{TopologyName, PackDomain}`.

So KServe could *propagate* a topology label, but it cannot guarantee end-to-end placement of an entire disaggregated pipeline within a single physical scope. That requires:

1. A scheduler that understands hierarchical gang requests (Grove uses Kai, Yunikorn, KubeRay-scheduler, etc.).
2. A workload-level API to express the constraint at the parent level.

Today KServe has neither; LWS does not have native topology hierarchies, and KServe does not generate `PodGang`/PodCliqueSet objects.

---

## 7. Bottlenecks for "deep" Grove integration into KServe

A "shallow" Grove integration is straightforward: package Dynamo (incl. NIXL, KV router, SLO planner) inside a single KServe runtime container. KServe treats Dynamo like any other container. This gives you Dynamo's per-node optimizations but bypasses Grove's PodCliqueSet entirely.

A "deep" integration — letting KServe generate `PodCliqueSet` resources and hand orchestration to Grove — has several hard constraints to negotiate.

### 7.1 Controller responsibility carve-out
KServe currently:

- Generates LWS objects with explicit `Spec.Replicas`, `Spec.LeaderWorkerTemplate`, etc.
- Wires storage initializers, PVCs, OCI volumes into the leader/worker templates (`attachModelArtifacts`).
- Manages a `ClusterIP` service plus the `llm-d` InferencePool selector.
- Wires WVA + HPA / KEDA targets to LWS.
- Tracks readiness via LWS conditions (`LeaderWorkerSetAvailable`, `LeaderWorkerSetProgressing`).

For Grove integration, every one of these branches needs a Grove-aware equivalent. The PCS spec is **richer** than LWS (cliques, scaling groups, topology constraints, startup ordering), so KServe would have to gain a translation layer from `LLMInferenceService.spec` to a `PodCliqueSet`. The most invasive change is the **status surface**: today KServe reads `LeaderWorkerSetAvailable`; under Grove it would need to interpret `PodCliqueSet.status.conditions`, `PodGangStatus.phase`, and per-clique status. The lifecycle semantics differ (Grove has `PodGangPending → Starting → Running`).

### 7.2 Network and gateway model conflict
The `llm-d` EPP routes via the standard `InferencePool` + `HTTPRoute` model on a Gateway. The KServe controller is built around this assumption (`router.go`, `router_discovery.go`, the routing-sidecar plumbing).

Dynamo has its own KV-aware router. If we hand orchestration to Grove and Dynamo's router, KServe must:

- Step back from L7 routing decisions (canary splits, prefix-aware scoring, etc.) and act as an L4 entry point.
- Disable / not inject the `llm-d-routing-sidecar` (it would conflict with Dynamo's path).
- Possibly hand over service discovery: Dynamo discovers workers via ETCD; KServe relies on Kubernetes Service / InferencePool selectors.

This is the most user-visible breaking change. The KServe canary-rollout feature, TLS termination, and observability hooks all assume the `llm-d` Gateway model.

### 7.3 Autoscaling
KServe today wires the WVA + HPA chain to the LWS. WVA reads vLLM/SGLang metrics over the model-server protocol. Dynamo has its own SLO Planner with a different metric surface and a different scaling decision loop. Running both at once on the same workload would create scale-target contention. So KServe would have to detect "this is a Grove-backed runtime" and **not** create the WVA / HPA / ScaledObject resources — leaving Grove + SLO Planner to scale the PodCliqueScalingGroups.

The decision logic is not trivial because PCSG-level scaling, PCLQ-level scaling, and PCS-level scaling do not map 1:1 to a single HPA scale target. KServe's `Spec.Scaling.WVA.HPA` would need to be reinterpreted as a request that Grove's scaling system honors.

### 7.4 CRD coupling and operability
Today KServe checks at controller startup whether the LWS CRD is available:

```365:366:kserve/pkg/controller/v1alpha2/llmisvc/controller.go
	if ok, err := utils.IsCrdAvailable(mgr.GetConfig(), lwsapi.GroupVersion.String(), "LeaderWorkerSet"); ok && err == nil {
		b = b.Owns(&lwsapi.LeaderWorkerSet{}, builder.WithPredicates(childResourcesPredicate))
```

For Grove, this expands to: `PodCliqueSet`, `PodClique`, `PodCliqueScalingGroup`, `PodGang`, `ClusterTopology`. Plus Grove requires its custom scheduler backend to be functional. So the install surface for "KServe with deep Grove integration" includes Grove operator + Grove scheduler + Kai/Yunikorn / a custom scheduler + `ClusterTopology` CRs administered by cluster admins. This is a meaningful step up from the standard "install KServe + LWS" model.

### 7.5 Readiness state machine mismatch
Grove's pod readiness is layered: the init container (`grove-initc`) blocks each pod's Ready state until parent cliques are at `MinAvailable`. So a pod can be `Running` for a long time before it is `Ready` — by design. KServe's "MarkWorkerWorkloadReady" today only considers `LeaderWorkerSetAvailable`. Under Grove, KServe must understand:

- `PodGangConditionTypeInitialized` (all pods created, scheduling gates removed).
- `PodGangConditionTypeScheduled` (all pods bound).
- `PodGangConditionTypeReady` (all pod groups Ready).
- `PodGangConditionTypeUnhealthy` (gang termination candidate).

Otherwise KServe will either prematurely mark the workload Ready (and route traffic into a half-initialized pipeline) or never mark it Ready (and time out).


## 8. Summary

The three stacks compare as follows on the capabilities that matter for disaggregated inference.

**Multi-node serving.** All three support it. KServe gets it from LWS's per-replica gang. Grove gets it from `PodGang`. `DisaggregatedSet` inherits LWS's behavior unchanged.

**Per-role independent autoscaling.** All three support it. KServe wires WVA + HPA / KEDA per LWS. Grove scales per `PodClique` / `PodCliqueScalingGroup`. `DisaggregatedSet` leaves autoscaling to the same KServe machinery and just exposes per-role replica counts.

**Hierarchical gang scheduling.** Only Grove. KServe and `DisaggregatedSet` both rely on the default scheduler, so prefill and decode are admitted independently — half a pipeline can come up while the other half stays Pending.

**Startup ordering across roles.** Only Grove, via its `grove-initc` init container that blocks pods until their declared parents are Ready. KServe relies on pods crashing-and-retrying until the topology converges; `DisaggregatedSet` doesn't address this.

**Rolling updates with P/D version isolation.** This is `DisaggregatedSet`'s headline feature: per-revision headless services and the N-dimensional planner mean a `v2` Prefill is never paired with a `v1` Decode mid-rollout. KServe alone is exposed to split-brain. Grove solves the same problem at the replica level with `RollingRecreate`.

**Topology-aware placement (NVLink / RDMA).** Only Grove offers first-class hardware topology constraints (`ClusterTopology`, `TopologyConstraint`). KServe and `DisaggregatedSet` can propagate Kueue / exclusive-topology labels but the scheduler doesn't understand cross-role NVLink/RDMA boundaries.

**Backend support.** KServe and `DisaggregatedSet` use vLLM as the primary engine with SGLang available through the routing sidecar. Grove supports vLLM, SGLang, and TensorRT-LLM via Dynamo runtimes.

**Native KV-aware routing.** KServe gets this from the `llm-d` EPP; `DisaggregatedSet` reuses the same EPP unchanged. Grove ships Dynamo's KV-Aware Router as a tightly integrated component.

**NVIDIA-specific dependencies.** KServe and `DisaggregatedSet` add none — they run on any accelerator and any cloud. Grove pulls in the Dynamo / NIXL / RDMA stack, which is NVIDIA-centric in practice.



1. Stay on KServe + LWS + `llm-d` (already production-grade for single-node and many multi-node cases).
2. Adopt `DisaggregatedSet` as an LWS sub-CRD so KServe can express prefill/decode as a single coordinated lifecycle and avoid split-brain rollouts.
3. Keep Grove on the radar for workloads where the scheduler-level guarantees (gang admission, NVLink packing, deterministic startup) actually pay off — typically 100B+ parameter models on dedicated NVIDIA hardware where the integration cost is justified by the perf delta.

