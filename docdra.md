# Managed DRA Deployment Examples

These examples show how to use the **Managed DRA** convenience feature on an `LLMInferenceService`
to request specialized devices (typically GPUs) via Kubernetes Dynamic Resource Allocation,
without authoring a `ResourceClaimTemplate` by hand.

> **Experimental.** Managed DRA is an intentionally limited-scope convenience layer for basic DRA
> use cases. The annotation keys are prefixed with `exp-` to make this explicit. Complex DRA
> topologies should use native Kubernetes `ResourceClaimTemplate` objects directly.

## Prerequisites

- Kubernetes **v1.34+** with the `DynamicResourceAllocation` feature gate enabled (Beta in 1.34)
- A `DeviceClass` installed by your device driver (for example, `gpu.nvidia.com` from the
  NVIDIA DRA driver, or `gpu.example.com` from a vendor-specific driver)
- Driver pods running and healthy — `kubectl get resourceslices` should show device inventory
- KServe with the LLMInferenceService controller installed

Verify your cluster:

```bash
kubectl get deviceclasses
kubectl get resourceslices
```

## How Managed DRA Works

When the `serving.kserve.io/exp-dra-device-class` annotation is set on an `LLMInferenceService`,
the controller:

1. **Creates** a `ResourceClaimTemplate` named after the `LLMInferenceService` whose request
   matches the annotation values (device class, count, optional CEL selectors).
2. **Injects** a corresponding `resourceClaims` entry into the pod template, and adds a
   `claims:` reference to the target container's `resources` field.
3. **Reconciles** updates and **cleans up** the template when the annotation is removed
   or the `LLMInferenceService` is deleted.

The container that receives the claim is the **first container** in the pod template by
default, or the one named by `exp-dra-container-name` if that annotation is set.

## Annotations

| Annotation                                    | Required | Default                | Description                                                                                                  |
|-----------------------------------------------|----------|------------------------|--------------------------------------------------------------------------------------------------------------|
| `serving.kserve.io/exp-dra-device-class`      | yes      | —                      | Name of the `DeviceClass` to allocate from. Must be a valid DNS subdomain. Setting this enables Managed DRA. |
| `serving.kserve.io/exp-dra-device-count`      | no       | `1`                    | Number of devices to request. Must be `>= 1`.                                                                |
| `serving.kserve.io/exp-dra-cel-selector`      | no       | —                      | Newline-separated CEL expressions that filter eligible devices. All expressions must evaluate to true (AND). |
| `serving.kserve.io/exp-dra-container-name`    | no       | first container        | Name of the container in the pod template that should receive the resource claim.                           |

The webhook rejects values that are syntactically invalid (non-numeric counts, malformed device
class names, container names that are not DNS labels, etc.) before they reach the controller.

## Examples

### 1. Minimal — single device, no filters

[`llm-inference-service-managed-dra-minimal.yaml`](llm-inference-service-managed-dra-minimal.yaml)

The smallest possible Managed DRA config: just `exp-dra-device-class`. Requests one device of
class `gpu.nvidia.com` and injects it into the only container in the pod template.

```yaml
metadata:
  annotations:
    serving.kserve.io/exp-dra-device-class: gpu.nvidia.com
```

```bash
kubectl apply -f llm-inference-service-managed-dra-minimal.yaml
```

### 2. With CEL selectors — filter eligible devices

[`llm-inference-service-managed-dra-cel-selectors.yaml`](llm-inference-service-managed-dra-cel-selectors.yaml)

Restricts allocation to devices that match **all** of the listed CEL expressions. Use the YAML
`|` block scalar so newlines are preserved verbatim in the annotation value:

```yaml
metadata:
  annotations:
    serving.kserve.io/exp-dra-device-class: gpu.nvidia.com
    serving.kserve.io/exp-dra-cel-selector: |
      device.attributes['gpu.nvidia.com'].productName == 'NVIDIA-A100-SXM4-80GB'
      device.capacity['gpu.nvidia.com'].memory.compareTo(quantity('40Gi')) >= 0
```

Each non-empty line becomes one CEL selector on the `DeviceRequest`. The CEL runtime is the
standard one used by Kubernetes DRA; see the
[DRA CEL reference](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/#device-selectors)
for the available variables (`device.attributes`, `device.capacity`, `device.driver`, etc.).

```bash
kubectl apply -f llm-inference-service-managed-dra-cel-selectors.yaml
```

### 3. Multi-device with explicit container targeting

[`llm-inference-service-managed-dra-multi-device.yaml`](llm-inference-service-managed-dra-multi-device.yaml)

Requests two devices and targets a specific container in a multi-container pod. Without the
`exp-dra-container-name` annotation, the claim would be injected into the first container
(`sidecar`) — which is not what we want for an inference workload. Setting the annotation
to `vllm` directs the claim to the actual model server container.

```yaml
metadata:
  annotations:
    serving.kserve.io/exp-dra-device-class: gpu.nvidia.com
    serving.kserve.io/exp-dra-device-count: "2"
    serving.kserve.io/exp-dra-container-name: vllm
    serving.kserve.io/exp-dra-cel-selector: |
      device.capacity['gpu.nvidia.com'].memory.compareTo(quantity('40Gi')) >= 0
```

```bash
kubectl apply -f llm-inference-service-managed-dra-multi-device.yaml
```

## Verification

After applying any of the manifests above:

```bash
# 1. Confirm the LLMInferenceService is healthy
kubectl get llminferenceservice <name>

# 2. The controller should have created a ResourceClaimTemplate of the same name
kubectl get resourceclaimtemplate <name> -o yaml

# 3. Each pod should have a ResourceClaim allocated to it
kubectl get pods -l app.kubernetes.io/name=<name>
kubectl get resourceclaims

# 4. The target container should reference the claim under resources.claims
kubectl get pod <pod> -o jsonpath='{.spec.containers[*].resources.claims}' | jq
```

## Cleanup

The controller manages the `ResourceClaimTemplate` lifecycle. Removing the
`exp-dra-device-class` annotation, or deleting the `LLMInferenceService`, removes the
template. There is no need to delete it manually.

```bash
kubectl delete -f llm-inference-service-managed-dra-minimal.yaml
```

## When *not* to use Managed DRA

Managed DRA is intentionally narrow. Reach for a hand-authored `ResourceClaimTemplate` if you
need any of:

- Multiple distinct device requests in one claim (e.g. one GPU **and** one NIC)
- `firstAvailable` ordered request lists for fallback hardware
- Cross-pod claim sharing or `ResourceClaim` (non-template) lifetimes
- Admin-controlled access constraints

In those cases create the `ResourceClaimTemplate` yourself and reference it from
`spec.template.spec.resourceClaims[].resourceClaimTemplateName` in the `LLMInferenceService`
pod template — the controller will leave hand-authored claims alone.

## Troubleshooting

### `LLMInferenceService` is rejected by the admission webhook

The webhook validates the four `exp-dra-*` annotations together. Common rejections:

- `exp-dra-device-count` is non-numeric, zero, or negative
- `exp-dra-device-class` contains uppercase letters or is otherwise not a DNS subdomain
- `exp-dra-container-name` is empty, contains uppercase letters, dots, or underscores
- `exp-dra-device-count`, `exp-dra-cel-selector`, or `exp-dra-container-name` is set without
  `exp-dra-device-class` also being set

The error message names the offending annotation and the failing rule.

### Pods stay `Pending` with `WaitingForDRA` / `UnschedulableDueToInsufficientResources`

The CEL selectors don't match any device in the cluster, or the requested count exceeds
available devices. Check what the driver published:

```bash
kubectl get resourceslices -o yaml
```

Then test your selector against that data, or relax the CEL expressions.

### `ResourceClaimTemplate` is not created

Confirm the `resource.k8s.io` API is enabled on your cluster
(`kubectl api-resources | grep resource.k8s.io`) and that the LLMInferenceService controller
has RBAC for `resourceclaims` and `resourceclaimtemplates` in that group. Both are included in
the standard KServe install manifests.
