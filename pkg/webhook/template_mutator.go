package webhook

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	"github.com/openshift/instaslice-operator/pkg/constants"
)

// MIGMutationResult contains the results of mutating a pod template for MIG resources.
type MIGMutationResult struct {
	// NeedsDASScheduler indicates whether the DAS scheduler should be used.
	NeedsDASScheduler bool
	// TotalGPUMemoryGB is the total GPU memory in GB across all MIG profiles.
	TotalGPUMemoryGB int64
	// MIGProfiles maps MIG profile names to their requested quantities.
	MIGProfiles map[string]int64
}

// ExtractMIGProfilesFromContainers extracts MIG profiles from container resource limits.
// Returns the profiles map and total GPU memory in GB.
func ExtractMIGProfilesFromContainers(containers []corev1.Container) (map[string]int64, int64) {
	profiles := make(map[string]int64)
	totalMemGB := int64(0)

	for _, c := range containers {
		if c.Resources.Limits == nil {
			continue
		}
		for name, qty := range c.Resources.Limits {
			key := string(name)
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

			memGB := extractGPUMemoryFromProfile(profile)
			if memGB > 0 {
				totalMemGB += memGB * quantity
			}
		}
	}

	return profiles, totalMemGB
}

// ExtractMIGProfilesFromInitContainers extracts MIG profiles from init containers.
func ExtractMIGProfilesFromInitContainers(initContainers []corev1.Container) (map[string]int64, int64) {
	return ExtractMIGProfilesFromContainers(initContainers)
}

// ExtractMIGProfilesFromEphemeralContainers extracts MIG profiles from ephemeral containers.
func ExtractMIGProfilesFromEphemeralContainers(ephemeralContainers []corev1.EphemeralContainer) (map[string]int64, int64) {
	// Convert ephemeral containers to regular containers for extraction
	containers := make([]corev1.Container, 0, len(ephemeralContainers))
	for _, ec := range ephemeralContainers {
		containers = append(containers, corev1.Container{
			Name:      ec.Name,
			Resources: ec.Resources,
		})
	}
	return ExtractMIGProfilesFromContainers(containers)
}

// ExtractMIGProfilesFromPodSpec extracts all MIG profiles from a PodSpec.
// It combines profiles from regular, init, and ephemeral containers.
func ExtractMIGProfilesFromPodSpec(podSpec *corev1.PodSpec) (map[string]int64, int64) {
	allProfiles := make(map[string]int64)
	totalMemGB := int64(0)

	// Regular containers
	profiles, memGB := ExtractMIGProfilesFromContainers(podSpec.Containers)
	for p, q := range profiles {
		allProfiles[p] += q
	}
	totalMemGB += memGB

	// Init containers
	profiles, memGB = ExtractMIGProfilesFromInitContainers(podSpec.InitContainers)
	for p, q := range profiles {
		allProfiles[p] += q
	}
	totalMemGB += memGB

	// Ephemeral containers
	profiles, memGB = ExtractMIGProfilesFromEphemeralContainers(podSpec.EphemeralContainers)
	for p, q := range profiles {
		allProfiles[p] += q
	}
	totalMemGB += memGB

	return allProfiles, totalMemGB
}

// SerializeMIGProfiles converts a profile map to a deterministic annotation string.
// Format: "1g.5gb:1,2g.10gb:2"
func SerializeMIGProfiles(profiles map[string]int64) string {
	if len(profiles) == 0 {
		return ""
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(profiles))
	for k := range profiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(profiles))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, profiles[k]))
	}

	return strings.Join(parts, ",")
}

// HasMIGResources checks if a PodSpec contains any MIG resource requests.
func HasMIGResources(podSpec *corev1.PodSpec) bool {
	profiles, _ := ExtractMIGProfilesFromPodSpec(podSpec)
	return len(profiles) > 0
}

// InjectGPUMemoryResource adds the GPU memory extended resource to container limits/requests.
func InjectGPUMemoryResource(resources *corev1.ResourceRequirements, totalMemGB int64) {
	if totalMemGB <= 0 {
		return
	}

	gpuMemResource := corev1.ResourceName(constants.GPUMemoryResource)
	gpuMemQuantity := resource.NewQuantity(totalMemGB, resource.DecimalSI)

	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}

	resources.Limits[gpuMemResource] = *gpuMemQuantity
	resources.Requests[gpuMemResource] = *gpuMemQuantity

	klog.InfoS("injected GPU memory resource", "resource", constants.GPUMemoryResource, "totalMemoryGB", totalMemGB)
}

// TransformContainerResources transforms NVIDIA MIG resources to DAS resources in a container.
// If isKueueManaged is true, MIG resources are removed (profiles go to annotation instead).
// If isKueueManaged is false, MIG resources are renamed to mig.das.com/*.
// Returns (totalGPUMemoryGB, needsScheduler).
func TransformContainerResources(resources *corev1.ResourceRequirements, isKueueManaged bool, migProfiles map[string]int64) (int64, bool) {
	if resources.Limits == nil {
		return 0, false
	}

	totalGPUMemory := int64(0)
	needsScheduler := false
	newLimits := corev1.ResourceList{}
	newRequests := corev1.ResourceList{}

	for name, qty := range resources.Limits {
		key := string(name)

		switch {
		case strings.HasPrefix(key, constants.NVIDIAMIGResourcePrefix):
			profile := strings.TrimPrefix(key, constants.NVIDIAMIGResourcePrefix)
			needsScheduler = true

			if !isKueueManaged {
				// For non-Kueue pods: rename to mig.das.com/*
				newKey := corev1.ResourceName(constants.MIGResourcePrefix + profile)
				klog.V(4).InfoS("renaming GPU resource", "from", key, "to", newKey)
				newLimits[newKey] = qty
				newRequests[newKey] = qty
			} else {
				// For Kueue pods: store in profiles map (will go to annotation)
				migProfiles[profile] += qty.Value()
				klog.V(4).InfoS("storing MIG profile for annotation", "profile", profile, "quantity", qty.Value())
			}

			// Calculate GPU memory
			memGB := extractGPUMemoryFromProfile(profile)
			if memGB > 0 {
				totalGPUMemory += memGB * qty.Value()
			}

		case strings.HasPrefix(key, constants.NVIDIAResourcePrefix):
			// Generic nvidia.com/* resource - rename to mig.das.com/*
			newKey := corev1.ResourceName(strings.Replace(key, constants.NVIDIAResourcePrefix, constants.MIGResourcePrefix, 1))
			klog.V(4).InfoS("renaming GPU resource", "from", key, "to", newKey)
			newLimits[newKey] = qty
			newRequests[newKey] = qty
			needsScheduler = true

		case strings.HasPrefix(key, constants.MIGResourcePrefix):
			// Already a mig.das.com/* resource
			newLimits[name] = qty
			needsScheduler = true

			// Extract memory from existing MIG profile
			profile := strings.TrimPrefix(key, constants.MIGResourcePrefix)
			memGB := extractGPUMemoryFromProfile(profile)
			if memGB > 0 {
				totalGPUMemory += memGB * qty.Value()
			}

		default:
			// Non-GPU resource - keep as-is
			newLimits[name] = qty
		}
	}

	// Copy non-GPU requests
	for name, qty := range resources.Requests {
		key := string(name)
		if !strings.HasPrefix(key, constants.NVIDIAResourcePrefix) &&
			!strings.HasPrefix(key, constants.MIGResourcePrefix) &&
			key != constants.GPUMemoryResource {
			newRequests[name] = qty
		}
	}

	// Update resources
	if len(newLimits) > 0 {
		resources.Limits = newLimits
	}
	if len(newRequests) > 0 {
		resources.Requests = newRequests
	}

	return totalGPUMemory, needsScheduler
}

// MutatePodTemplateForKueue mutates a PodTemplateSpec for Kueue workload admission.
// It injects GPU memory resources but does NOT transform MIG resources (Kueue needs originals).
// Returns the MIG profiles found and total GPU memory.
func MutatePodTemplateForKueue(template *corev1.PodTemplateSpec) (map[string]int64, int64) {
	profiles, totalMemGB := ExtractMIGProfilesFromPodSpec(&template.Spec)

	if totalMemGB > 0 {
		// For Kueue workloads, we inject GPU memory into the first container
		// This is what Kueue will use for quota/admission
		if len(template.Spec.Containers) > 0 {
			InjectGPUMemoryResource(&template.Spec.Containers[0].Resources, totalMemGB)
		}
	}

	return profiles, totalMemGB
}

