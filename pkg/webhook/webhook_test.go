package webhook

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/openshift/instaslice-operator/pkg/constants"
)

const (
	envNvidia = "NVIDIA_VISIBLE_DEVICES"
	envCUDA   = "CUDA_VISIBLE_DEVICES"
	testName  = "test"
)

func TestMutatePodNvidiaResource(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testName},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  testName,
					Image: "ubuntu:20.04",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("failed to unmarshal mutated pod: %v", err)
	}

	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected scheduler %s, got %s", constants.DASSchedulerName, mutated.Spec.SchedulerName)
	}

	if mutated.Spec.RuntimeClassName == nil || *mutated.Spec.RuntimeClassName != "nvidia-legacy" {
		t.Fatalf("expected runtimeClassName nvidia-legacy")
	}

	limits := mutated.Spec.Containers[0].Resources.Limits
	if _, ok := limits[corev1.ResourceName("nvidia.com/mig-1g.5gb")]; ok {
		t.Fatalf("nvidia resource still present")
	}
	q, ok := limits[corev1.ResourceName("mig.das.com/1g.5gb")]
	if !ok || q.Value() != 1 {
		t.Fatalf("expected instaslice resource quantity 1")
	}

	if len(mutated.Spec.Containers[0].Env) != 0 {
		t.Fatalf("env vars should not be added")
	}
}

func TestMutatePodEphemeralNvidiaResource(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ephem"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main"}},
			EphemeralContainers: []corev1.EphemeralContainer{
				{
					EphemeralContainerCommon: corev1.EphemeralContainerCommon{
						Name:  "debug",
						Image: "busybox",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
							},
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}
	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected scheduler set")
	}
	if mutated.Spec.RuntimeClassName == nil || *mutated.Spec.RuntimeClassName != "nvidia-legacy" {
		t.Fatalf("expected runtimeClassName nvidia-legacy")
	}
	limits := mutated.Spec.EphemeralContainers[0].Resources.Limits
	if _, ok := limits[corev1.ResourceName("mig.das.com/1g.5gb")]; !ok {
		t.Fatalf("instaslice resource missing")
	}

	if len(mutated.Spec.EphemeralContainers[0].Env) != 0 {
		t.Fatalf("env vars should not be added")
	}
}

func TestMutatePodOverrideValues(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "override"},
		Spec: corev1.PodSpec{
			SchedulerName: "foo",
			Containers: []corev1.Container{
				{
					Name:  "c",
					Image: "busybox",
					Env: []corev1.EnvVar{
						{Name: envNvidia, Value: "0"},
						{Name: envCUDA, Value: "0"},
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}

	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("scheduler not overridden")
	}
	if mutated.Spec.RuntimeClassName == nil || *mutated.Spec.RuntimeClassName != "nvidia-legacy" {
		t.Fatalf("expected runtimeClassName nvidia-legacy")
	}

	envs := mutated.Spec.Containers[0].Env
	if len(envs) != 2 {
		t.Fatalf("expected 2 env vars, got %d", len(envs))
	}
	for _, e := range envs {
		switch e.Name {
		case envNvidia:
			if e.Value != "0" {
				t.Fatalf("NVIDIA env not preserved")
			}
		case envCUDA:
			if e.Value != "0" {
				t.Fatalf("CUDA env not preserved")
			}
		default:
			t.Fatalf("unexpected env var %s", e.Name)
		}
	}
}

func TestMutatePodInstaResource(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testName},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  testName,
					Image: "ubuntu:20.04",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("mig.das.com/1g.5gb"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}
	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}
	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected scheduler set")
	}
	if mutated.Spec.RuntimeClassName == nil || *mutated.Spec.RuntimeClassName != "nvidia-legacy" {
		t.Fatalf("expected runtimeClassName nvidia-legacy")
	}
	if _, ok := mutated.Spec.Containers[0].Resources.Limits[corev1.ResourceName("mig.das.com/1g.5gb")]; !ok {
		t.Fatalf("instaslice resource missing")
	}

	if len(mutated.Spec.Containers[0].Env) != 0 {
		t.Fatalf("env vars should not be added")
	}
}

func TestMutatePodNoResource(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "none"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "t"}},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}
	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}
	if mutated.Spec.SchedulerName != "" {
		t.Fatalf("expected scheduler not set")
	}
	if mutated.Spec.RuntimeClassName != nil {
		t.Fatalf("expected runtimeClassName not set for non-GPU pod")
	}
	if len(mutated.Spec.Containers[0].Env) != 0 {
		t.Fatalf("env vars should not be added")
	}
}

func TestMutatePodKueueManaged(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kueue-pod",
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": "test-queue",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "ubuntu:20.04",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}

	limits := mutated.Spec.Containers[0].Resources.Limits

	// For Kueue-managed Pods: Should NOT have mig.das.com/* resources
	if _, exists := limits[corev1.ResourceName("mig.das.com/1g.5gb")]; exists {
		t.Fatalf("Kueue-managed Pod should NOT have mig.das.com/* resources")
	}

	// For Kueue-managed Pods: Should have gpu.das.openshift.io/mem
	gpuMem, exists := limits[corev1.ResourceName("gpu.das.openshift.io/mem")]
	if !exists {
		t.Fatalf("Kueue-managed Pod should have gpu.das.openshift.io/mem resource")
	}
	if gpuMem.Value() != 5 {
		t.Fatalf("expected GPU memory 5GB, got %d", gpuMem.Value())
	}

	// Should use DAS scheduler
	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected DAS scheduler to be set")
	}
}

func TestMutatePodNonKueue(t *testing.T) {
	hook := &InstasliceWebhook{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "non-kueue-pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "ubuntu:20.04",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}

	limits := mutated.Spec.Containers[0].Resources.Limits

	// For non-Kueue Pods: Should have mig.das.com/* resources (current DAS logic)
	migResource, exists := limits[corev1.ResourceName("mig.das.com/1g.5gb")]
	if !exists {
		t.Fatalf("non-Kueue Pod should have mig.das.com/* resources")
	}
	if migResource.Value() != 1 {
		t.Fatalf("expected MIG resource quantity 1, got %d", migResource.Value())
	}

	// For non-Kueue Pods: Should also have gpu.das.openshift.io/mem
	gpuMem, exists := limits[corev1.ResourceName("gpu.das.openshift.io/mem")]
	if !exists {
		t.Fatalf("non-Kueue Pod should have gpu.das.openshift.io/mem resource")
	}
	if gpuMem.Value() != 5 {
		t.Fatalf("expected GPU memory 5GB, got %d", gpuMem.Value())
	}

	// Should use DAS scheduler
	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected DAS scheduler to be set")
	}
}

// TestMutateWorkloadWithInitContainers tests workload mutation with init containers.
// Init containers run sequentially, so we take max GPU memory among them.
// Regular containers run concurrently, so we sum their GPU memory.
// Effective GPU memory = max(max_init_gpu, sum_regular_gpu)
func TestMutateWorkloadWithInitContainers(t *testing.T) {
	hook := &InstasliceWorkloadWebhook{}

	// Workload with:
	// - init container requiring 10GB (2g.10gb)
	// - regular container requiring 5GB (1g.5gb)
	// Expected: max(10, 5) = 10GB
	workload := &kueuev1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workload",
			Namespace: "default",
		},
		Spec: kueuev1beta1.WorkloadSpec{
			PodSets: []kueuev1beta1.PodSet{
				{
					Name:  "main",
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							InitContainers: []corev1.Container{
								{
									Name: "model-loader",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-2g.10gb"): resource.MustParse("1"),
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name: "trainer",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mutatedBytes, err := hook.mutateWorkload(workload)
	if err != nil {
		t.Fatalf("mutateWorkload returned error: %v", err)
	}

	var mutated kueuev1beta1.Workload
	if err := json.Unmarshal(mutatedBytes, &mutated); err != nil {
		t.Fatalf("failed to unmarshal mutated workload: %v", err)
	}

	// Check GPU memory was injected into first container
	limits := mutated.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits
	gpuMem, exists := limits[corev1.ResourceName(constants.GPUMemoryResource)]
	if !exists {
		t.Fatalf("gpu.das.openshift.io/mem should be injected")
	}

	// Init container has 10GB, regular has 5GB. max(10, 5) = 10
	if gpuMem.Value() != 10 {
		t.Fatalf("expected GPU memory 10GB (max of init=10, regular=5), got %d", gpuMem.Value())
	}

	// Check annotation contains both profiles
	migAnnotation, exists := mutated.Annotations[constants.MIGProfileAnnotation]
	if !exists {
		t.Fatalf("das.openshift.io/mig-profiles annotation should be set")
	}

	// Should contain both profiles: "1g.5gb:1,2g.10gb:1"
	if migAnnotation != "1g.5gb:1,2g.10gb:1" {
		t.Fatalf("expected annotation '1g.5gb:1,2g.10gb:1', got '%s'", migAnnotation)
	}
}

// TestMutateWorkloadInitContainerLargerThanRegular tests the case where
// init container requires more GPU memory than regular containers combined.
func TestMutateWorkloadInitContainerLargerThanRegular(t *testing.T) {
	hook := &InstasliceWorkloadWebhook{}

	// Workload with:
	// - init container requiring 20GB (3g.20gb)
	// - regular container requiring 5GB (1g.5gb)
	// Expected: max(20, 5) = 20GB
	workload := &kueuev1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workload-init-larger",
			Namespace: "default",
		},
		Spec: kueuev1beta1.WorkloadSpec{
			PodSets: []kueuev1beta1.PodSet{
				{
					Name:  "main",
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							InitContainers: []corev1.Container{
								{
									Name: "large-init",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-3g.20gb"): resource.MustParse("1"),
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name: "small-worker",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mutatedBytes, err := hook.mutateWorkload(workload)
	if err != nil {
		t.Fatalf("mutateWorkload returned error: %v", err)
	}

	var mutated kueuev1beta1.Workload
	if err := json.Unmarshal(mutatedBytes, &mutated); err != nil {
		t.Fatalf("failed to unmarshal mutated workload: %v", err)
	}

	limits := mutated.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits
	gpuMem := limits[corev1.ResourceName(constants.GPUMemoryResource)]

	// Init container has 20GB which is larger than regular 5GB
	if gpuMem.Value() != 20 {
		t.Fatalf("expected GPU memory 20GB (init container is larger), got %d", gpuMem.Value())
	}
}

// TestMutateWorkloadMultipleInitContainersSequential tests that we take the max
// among multiple init containers (since they run sequentially).
func TestMutateWorkloadMultipleInitContainersSequential(t *testing.T) {
	hook := &InstasliceWorkloadWebhook{}

	// Workload with:
	// - init container 1 requiring 5GB (1g.5gb)
	// - init container 2 requiring 20GB (3g.20gb)
	// - regular container requiring 10GB (2g.10gb)
	// Expected: max(max(5, 20), 10) = max(20, 10) = 20GB
	workload := &kueuev1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workload-multi-init",
			Namespace: "default",
		},
		Spec: kueuev1beta1.WorkloadSpec{
			PodSets: []kueuev1beta1.PodSet{
				{
					Name:  "main",
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							InitContainers: []corev1.Container{
								{
									Name: "init-small",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
										},
									},
								},
								{
									Name: "init-large",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-3g.20gb"): resource.MustParse("1"),
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name: "worker",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-2g.10gb"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mutatedBytes, err := hook.mutateWorkload(workload)
	if err != nil {
		t.Fatalf("mutateWorkload returned error: %v", err)
	}

	var mutated kueuev1beta1.Workload
	if err := json.Unmarshal(mutatedBytes, &mutated); err != nil {
		t.Fatalf("failed to unmarshal mutated workload: %v", err)
	}

	limits := mutated.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits
	gpuMem := limits[corev1.ResourceName(constants.GPUMemoryResource)]

	// max(max(5, 20), 10) = 20
	if gpuMem.Value() != 20 {
		t.Fatalf("expected GPU memory 20GB (max of init containers), got %d", gpuMem.Value())
	}
}

// TestMutatePodGPUMemoryAlreadyInjected tests that if gpu.das.openshift.io/mem
// is already present (e.g., from Workload webhook), it's not re-injected.
func TestMutatePodGPUMemoryAlreadyInjected(t *testing.T) {
	hook := &InstasliceWebhook{}

	// Simulate a Pod that came from a Kueue Workload where the Workload webhook
	// already injected gpu.das.openshift.io/mem
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-from-workload",
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": "test-queue",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "trainer",
					Image: "pytorch:latest",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							// MIG profile from original workload
							corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
							// GPU memory already injected by Workload webhook
							corev1.ResourceName(constants.GPUMemoryResource): resource.MustParse("10"),
						},
					},
				},
			},
		},
	}

	data, err := hook.mutatePod(pod)
	if err != nil {
		t.Fatalf("mutatePod returned error: %v", err)
	}

	mutated := &corev1.Pod{}
	if err := json.Unmarshal(data, mutated); err != nil {
		t.Fatalf("unmarshal mutated pod: %v", err)
	}

	limits := mutated.Spec.Containers[0].Resources.Limits

	// GPU memory should remain at 10 (from Workload webhook), not be overwritten to 5
	gpuMem, exists := limits[corev1.ResourceName(constants.GPUMemoryResource)]
	if !exists {
		t.Fatalf("gpu.das.openshift.io/mem should still be present")
	}

	// The value should be preserved from the original (10GB from Workload webhook)
	// not recalculated to 5GB from the single 1g.5gb profile
	if gpuMem.Value() != 10 {
		t.Fatalf("expected GPU memory to remain at 10GB (from Workload webhook), got %d", gpuMem.Value())
	}

	// Should still use DAS scheduler
	if mutated.Spec.SchedulerName != constants.DASSchedulerName {
		t.Fatalf("expected DAS scheduler to be set")
	}

	// MIG profile annotation should still be added
	if mutated.Annotations == nil || mutated.Annotations[constants.MIGProfileAnnotation] == "" {
		t.Fatalf("MIG profile annotation should be set for Kueue-managed Pod")
	}
}

// TestMutateWorkloadRegularContainersConcurrent tests that regular containers
// are summed (since they run concurrently).
func TestMutateWorkloadRegularContainersConcurrent(t *testing.T) {
	hook := &InstasliceWorkloadWebhook{}

	// Workload with:
	// - regular container 1 requiring 5GB (1g.5gb)
	// - regular container 2 requiring 10GB (2g.10gb)
	// Expected: sum(5, 10) = 15GB
	workload := &kueuev1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-workload-concurrent",
			Namespace: "default",
		},
		Spec: kueuev1beta1.WorkloadSpec{
			PodSets: []kueuev1beta1.PodSet{
				{
					Name:  "main",
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "worker1",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-1g.5gb"): resource.MustParse("1"),
										},
									},
								},
								{
									Name: "worker2",
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/mig-2g.10gb"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mutatedBytes, err := hook.mutateWorkload(workload)
	if err != nil {
		t.Fatalf("mutateWorkload returned error: %v", err)
	}

	var mutated kueuev1beta1.Workload
	if err := json.Unmarshal(mutatedBytes, &mutated); err != nil {
		t.Fatalf("failed to unmarshal mutated workload: %v", err)
	}

	limits := mutated.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits
	gpuMem := limits[corev1.ResourceName(constants.GPUMemoryResource)]

	// sum(5, 10) = 15
	if gpuMem.Value() != 15 {
		t.Fatalf("expected GPU memory 15GB (sum of concurrent containers), got %d", gpuMem.Value())
	}
}
