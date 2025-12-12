package webhook

import (
	"encoding/json"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"

	admissionctl "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/openshift/instaslice-operator/pkg/constants"
)

const (
	WorkloadURI         string = "/mutate-workload"
	WorkloadWebhookName string = "das-workload-webhook"
)

// InstasliceWorkloadWebhook handles mutation of Kueue Workload resources
type InstasliceWorkloadWebhook struct{}

// NewWorkloadWebhook creates a new WorkloadWebhook instance
func NewWorkloadWebhook() *InstasliceWorkloadWebhook {
	return &InstasliceWorkloadWebhook{}
}

// GetURI implements GenericWebhook interface
func (w *InstasliceWorkloadWebhook) GetURI() string { return WorkloadURI }

// Name implements GenericWebhook interface
func (w *InstasliceWorkloadWebhook) Name() string { return WorkloadWebhookName }

// Authorized implements GenericWebhook interface
func (w *InstasliceWorkloadWebhook) Authorized(request admissionctl.Request) admissionctl.Response {
	var ret admissionctl.Response

	klog.InfoS("Workload Webhook called", "uid", request.UID, "kind", request.Kind.Kind,
		"name", request.Name, "namespace", request.Namespace)

	workload, err := w.renderWorkload(request)
	if err != nil {
		klog.ErrorS(err, "Failed to render Workload from request", "uid", request.UID)
		ret = admissionctl.Errored(http.StatusBadRequest, err)
		ret.UID = request.UID
		return ret
	}

	klog.InfoS("Rendering Workload successful", "name", workload.Name, "namespace", workload.Namespace)

	mutatedWorkload, err := w.mutateWorkload(workload)
	if err != nil {
		klog.ErrorS(err, "Workload mutation failed", "uid", request.UID)
		ret = admissionctl.Errored(http.StatusBadRequest, err)
		ret.UID = request.UID
		return ret
	}
	klog.InfoS("Workload mutation successful", "name", workload.Name, "namespace", workload.Namespace)

	ret = admissionctl.PatchResponseFromRaw(request.Object.Raw, mutatedWorkload)
	ret.UID = request.UID
	klog.V(4).InfoS("Returning patch response for Workload", "uid", request.UID, "patch", string(ret.Patch))
	return ret
}

// mutateWorkload mutates the Workload resource to add GPU memory for Kueue admission.
//
// GPU memory calculation follows Kubernetes pod resource semantics:
// - Init containers run sequentially before regular containers → take max GPU memory among them
// - Regular containers run concurrently → sum their GPU memory requirements
// - Ephemeral containers are ignored for scheduling (they're added post-creation)
// - Pod's effective GPU memory = max(max_init_gpu, sum_regular_gpu)
func (w *InstasliceWorkloadWebhook) mutateWorkload(workload *kueuev1beta1.Workload) ([]byte, error) {
	if len(workload.Spec.PodSets) == 0 {
		klog.V(4).InfoS("No podSets found in Workload", "name", workload.Name)
		return json.Marshal(workload)
	}

	klog.InfoS("Mutating Workload", "name", workload.Name, "namespace", workload.Namespace, "podSets", len(workload.Spec.PodSets))

	allProfiles := make(map[string]int64)
	totalMemGB := int64(0)
	modified := false

	// Process each podSet
	for i := range workload.Spec.PodSets {
		podSet := &workload.Spec.PodSets[i]

		count := int64(podSet.Count)
		if count == 0 {
			count = 1
			klog.V(4).InfoS("PodSet count is 0, defaulting to 1",
				"workload", workload.Name, "podSet", podSet.Name)
		}

		podSetProfiles := make(map[string]int64)

		// Process regular containers (they run concurrently, so sum their resources)
		regularMemGB, regularProfiles := w.extractMIGFromContainersConcurrent(podSet.Template.Spec.Containers)
		for p, q := range regularProfiles {
			podSetProfiles[p] += q
		}

		// Process init containers (they run sequentially, so take max resource per profile)
		// extractMIGFromContainersSequential returns max profiles (not sum) since init containers
		// run one at a time.
		initMaxMemGB, initProfiles := w.extractMIGFromContainersSequential(podSet.Template.Spec.InitContainers)
		for p, q := range initProfiles {
			// Take max between init container peak and regular container sum for each profile.
			// This is because during pod execution, either init containers OR regular containers
			// are running, never both simultaneously.
			if q > podSetProfiles[p] {
				podSetProfiles[p] = q
			}
		}

		// Pod's effective GPU memory follows Kubernetes semantics:
		// max(max_init_container, sum_regular_containers)
		// This is because init containers complete before regular containers start
		podSetMemGB := regularMemGB
		if initMaxMemGB > podSetMemGB {
			podSetMemGB = initMaxMemGB
		}

		// Inject GPU memory into the first regular container if we found MIG profiles
		if podSetMemGB > 0 && len(podSet.Template.Spec.Containers) > 0 {
			container := &podSet.Template.Spec.Containers[0]

			if container.Resources.Limits == nil {
				container.Resources.Limits = corev1.ResourceList{}
			}
			if container.Resources.Requests == nil {
				container.Resources.Requests = corev1.ResourceList{}
			}

			gpuMemResource := corev1.ResourceName(constants.GPUMemoryResource)
			gpuMemQuantity := resource.NewQuantity(podSetMemGB, resource.DecimalSI)

			container.Resources.Limits[gpuMemResource] = *gpuMemQuantity
			container.Resources.Requests[gpuMemResource] = *gpuMemQuantity
			modified = true

			klog.V(4).InfoS("Injected GPU memory into first container",
				"podSet", podSet.Name, "container", container.Name, "memoryGB", podSetMemGB)
		}

		// NOTE: Original nvidia.com/mig-* resources are NOT removed here.
		// For Kueue-managed Jobs, the Job webhook transforms resources before the Workload is created.
		// For standalone Workloads, we add gpu.das.openshift.io/mem alongside existing resources.

		// Accumulate profiles (considering podSet count)
		for profile, qty := range podSetProfiles {
			allProfiles[profile] += qty * count
		}
		totalMemGB += podSetMemGB * count

		klog.V(4).InfoS("Processed podSet", "name", podSet.Name, "profiles", podSetProfiles,
			"regularMemGB", regularMemGB, "initMaxMemGB", initMaxMemGB,
			"effectiveMemGB", podSetMemGB, "count", count)
	}

	// If no MIG profiles found, return unchanged
	if len(allProfiles) == 0 {
		klog.InfoS("No MIG profiles found in Workload, allowing without modification", "name", workload.Name)
		return json.Marshal(workload)
	}

	// Add MIG profiles annotation if we modified the workload
	if modified {
		if workload.Annotations == nil {
			workload.Annotations = make(map[string]string)
		}
		workload.Annotations[constants.MIGProfileAnnotation] = SerializeMIGProfiles(allProfiles)

		klog.InfoS("Mutated Workload for Kueue admission",
			"name", workload.Name,
			"totalMemoryGB", totalMemGB,
			"profiles", allProfiles,
			"annotation", workload.Annotations[constants.MIGProfileAnnotation])
	}

	return json.Marshal(workload)
}

// extractMIGFromContainersConcurrent extracts MIG profiles from containers that run concurrently.
// Returns the sum of GPU memory across all containers and a map of all profiles.
func (w *InstasliceWorkloadWebhook) extractMIGFromContainersConcurrent(containers []corev1.Container) (int64, map[string]int64) {
	totalMemGB := int64(0)
	allProfiles := make(map[string]int64)

	for _, container := range containers {
		memGB, profiles := w.extractMIGFromContainer(&container)
		totalMemGB += memGB
		for p, q := range profiles {
			allProfiles[p] += q
		}
	}

	return totalMemGB, allProfiles
}

// extractMIGFromContainersSequential extracts MIG profiles from containers that run sequentially.
// Returns the maximum GPU memory among all containers (since only one runs at a time)
// and a map of max profiles per profile type (also max since only one container runs at a time).
//
// Example: If init1 needs 1g.5gb:2 and init2 needs 1g.5gb:1, we return 1g.5gb:2 (the max).
// This correctly reflects that the peak requirement is 2 slices, not 3.
func (w *InstasliceWorkloadWebhook) extractMIGFromContainersSequential(containers []corev1.Container) (int64, map[string]int64) {
	maxMemGB := int64(0)
	maxProfiles := make(map[string]int64)

	for _, container := range containers {
		memGB, profiles := w.extractMIGFromContainer(&container)
		if memGB > maxMemGB {
			maxMemGB = memGB
		}
		// Take max per profile since containers run sequentially
		for p, q := range profiles {
			if q > maxProfiles[p] {
				maxProfiles[p] = q
			}
		}
	}

	return maxMemGB, maxProfiles
}

// extractMIGFromContainer extracts MIG profiles and GPU memory from a single container.
func (w *InstasliceWorkloadWebhook) extractMIGFromContainer(container *corev1.Container) (int64, map[string]int64) {
	profiles := make(map[string]int64)
	memGB := int64(0)

	if container.Resources.Limits == nil {
		return 0, profiles
	}

	for resourceName, qty := range container.Resources.Limits {
		key := string(resourceName)
		var profile string

		switch {
		case strings.HasPrefix(key, constants.NVIDIAMIGResourcePrefix):
			profile = strings.TrimPrefix(key, constants.NVIDIAMIGResourcePrefix)
		case strings.HasPrefix(key, constants.MIGResourcePrefix):
			profile = strings.TrimPrefix(key, constants.MIGResourcePrefix)
		default:
			continue
		}

		quantity := qty.Value()
		profiles[profile] += quantity

		profileMemGB := extractGPUMemoryFromProfile(profile)
		if profileMemGB > 0 {
			memGB += profileMemGB * quantity
		}
	}

	return memGB, profiles
}

// renderWorkload decodes the Workload from the admission request
func (w *InstasliceWorkloadWebhook) renderWorkload(request admissionctl.Request) (*kueuev1beta1.Workload, error) {
	klog.V(4).InfoS("Rendering Workload from request", "uid", request.UID)

	workload := &kueuev1beta1.Workload{}
	// Always use Object.Raw (the new/current state) for both CREATE and UPDATE operations.
	// Using OldObject.Raw on UPDATE would cause us to lose changes made by Kueue.
	if err := json.Unmarshal(request.Object.Raw, workload); err != nil {
		return nil, err
	}

	return workload, nil
}

// Ensure InstasliceWorkloadWebhook implements GenericWebhook
var _ GenericWebhook = (*InstasliceWorkloadWebhook)(nil)
