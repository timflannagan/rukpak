package unpacker

import (
	"context"
	"fmt"

	olmv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/podutil"
	"github.com/operator-framework/rukpak/internal/updater"
	"github.com/operator-framework/rukpak/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type UnpackResult struct {
	Objects     []client.Object
	ImageDigest string
}

type Unpacker interface {
	UnpackBundle(context.Context, *olmv1alpha1.Bundle) (*UnpackResult, error)
}

var _ Unpacker = &PodBundleUnpacker{}

type PodBundleUnpacker struct {
	Client       client.Client
	KubeClient   kubernetes.Interface
	updater      updater.Updater
	PodNamespace string
	UnpackImage  string
}

func (u *PodBundleUnpacker) UnpackBundle(ctx context.Context, bundle *olmv1alpha1.Bundle) (*UnpackResult, error) {
	pod := &corev1.Pod{}

	op, err := u.ensureUnpackPodCRIO(ctx, bundle, pod)
	if err != nil {
		u.updater.UpdateStatus(updater.SetBundleInfo(nil), updater.EnsureBundleDigest(""))
		return nil, updater.UpdateStatusUnpackFailing(&u.updater, fmt.Errorf("ensure unpack pod: %w", err))
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated || pod.DeletionTimestamp != nil {
		updater.UpdateStatusUnpackPending(&u.updater)
		return nil, nil
	}

	if err := u.ensurePodIsReady(ctx, pod); err != nil {
		// TODO: handle this edge case correctly
		u.updater.UpdateStatus(updater.SetBundleInfo(nil), updater.EnsureBundleDigest(""))
		return nil, updater.UpdateStatusUnpackFailing(&u.updater, fmt.Errorf("ensure unpack pod: %w", err))
	}

	res, err := podutil.HandleCompletedPod(ctx, u.KubeClient, &u.updater, bundle, pod)
	if err != nil {
		return nil, err
	}

	var info *olmv1alpha1.BundleInfo
	if res.Annotations != nil {
		info = &olmv1alpha1.BundleInfo{Package: res.Annotations.PackageName}
	}
	for _, obj := range res.Objects {
		if obj.GetObjectKind().GroupVersionKind().Kind == "ClusterServiceVersion" {
			info.Name = obj.GetName()
			u := obj.(*unstructured.Unstructured)
			info.Version, _, _ = unstructured.NestedString(u.Object, "spec", "version")
		}
		gvk := obj.GetObjectKind().GroupVersionKind()
		info.Objects = append(info.Objects, olmv1alpha1.BundleObject{
			Group:     gvk.Group,
			Version:   gvk.Version,
			Kind:      gvk.Kind,
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		})
	}

	u.updater.UpdateStatus(
		updater.SetBundleInfo(info),
		updater.EnsureBundleDigest(res.ImageDigest),
		updater.SetPhase(olmv1alpha1.PhaseUnpacked),
		updater.EnsureCondition(metav1.Condition{
			Type:   olmv1alpha1.TypeUnpacked,
			Status: metav1.ConditionTrue,
			Reason: olmv1alpha1.ReasonUnpackSuccessful,
		}),
	)
	return &UnpackResult{
		Objects:     res.Objects,
		ImageDigest: res.ImageDigest,
	}, nil
}

func (u *PodBundleUnpacker) ensurePodIsReady(ctx context.Context, pod *corev1.Pod) error {
	phase := pod.Status.Phase
	if phase == corev1.PodPending {
		podutil.HandlePendingPod(&u.updater, pod)
		return nil
	}
	if phase == corev1.PodRunning {
		podutil.HandleRunningPod(&u.updater)
		return nil
	}
	if phase == corev1.PodFailed {
		return podutil.HandleFailedPod(ctx, u.Client, u.KubeClient, &u.updater, pod)
	}
	if phase != corev1.PodSucceeded {
		return podutil.HandleUnexpectedPod(ctx, u.Client, &u.updater, pod)
	}
	return nil
}

func (u *PodBundleUnpacker) ensureUnpackPodCRIO(ctx context.Context, bundle *olmv1alpha1.Bundle, pod *corev1.Pod) (controllerutil.OperationResult, error) {
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	automountServiceAccountToken := false
	pod.SetName(util.PodName(bundle.Name))
	pod.SetNamespace(u.PodNamespace)

	return util.CreateOrRecreate(ctx, u.Client, pod, func() error {
		pod.SetLabels(map[string]string{"kuberpak.io/owner-name": bundle.Name})
		pod.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
		pod.Spec.AutomountServiceAccountToken = &automountServiceAccountToken
		pod.Spec.Volumes = []corev1.Volume{
			{Name: "util", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "bundle", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		}
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever
		if len(pod.Spec.InitContainers) != 2 {
			pod.Spec.InitContainers = make([]corev1.Container, 2)
		}
		pod.Spec.InitContainers[0].Name = "install-cpb"
		pod.Spec.InitContainers[0].Image = "quay.io/operator-framework/olm:latest"
		pod.Spec.InitContainers[0].Command = []string{"/bin/cp", "-Rv", "/bin/cpb", "/util/cpb"}
		pod.Spec.InitContainers[0].VolumeMounts = []corev1.VolumeMount{{Name: "util", MountPath: "/util"}}

		pod.Spec.InitContainers[1].Name = "copy-bundle"
		pod.Spec.InitContainers[1].Image = bundle.Spec.Image
		pod.Spec.InitContainers[1].ImagePullPolicy = corev1.PullAlways
		pod.Spec.InitContainers[1].Command = []string{"/util/cpb", "/bundle"}
		pod.Spec.InitContainers[1].VolumeMounts = []corev1.VolumeMount{
			{Name: "util", MountPath: "/util"},
			{Name: "bundle", MountPath: "/bundle"},
		}

		if len(pod.Spec.Containers) != 1 {
			pod.Spec.Containers = make([]corev1.Container, 1)
		}
		pod.Spec.Containers[0].Name = "unpack-bundle"
		pod.Spec.Containers[0].Image = u.UnpackImage
		pod.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
		pod.Spec.Containers[0].Args = []string{"--bundle-dir=/bundle"}
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "bundle", MountPath: "/bundle"}}
		return nil
	})
}
