package podutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/operator-framework/operator-registry/pkg/registry"
	olmv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/updater"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

type PodUnpackResult struct {
	Objects     []client.Object
	ImageDigest string
	Annotations *registry.Annotations
}

func HandleCompletedPod(ctx context.Context, kc kubernetes.Interface, u *updater.Updater, bundle *olmv1alpha1.Bundle, pod *corev1.Pod) (*PodUnpackResult, error) {
	contents, err := GetPodLogs(ctx, kc, pod)
	if err != nil {
		return nil, updater.UpdateStatusUnpackFailing(u, fmt.Errorf("get pod logs: %w", err))
	}
	bundleFS, err := getBundleContents(ctx, contents)
	if err != nil {
		return nil, updater.UpdateStatusUnpackFailing(u, fmt.Errorf("get bundle contents: %w", err))
	}

	bundleImageDigest, err := getBundleImageDigest(pod)
	if err != nil {
		return nil, updater.UpdateStatusUnpackFailing(u, fmt.Errorf("get bundle image digest: %w", err))
	}

	// TODO(tflannag): Not entirely sure how we can avoid embedding registry+v1
	// annotation results in the UnpackResult.
	annotations, err := getAnnotations(bundleFS)
	if err != nil {
		return nil, updater.UpdateStatusUnpackFailing(u, fmt.Errorf("get bundle annotations: %w", err))
	}

	objects, err := getObjects(bundleFS)
	if err != nil {
		return nil, updater.UpdateStatusUnpackFailing(u, fmt.Errorf("get objects from bundle manifests: %w", err))
	}

	return &PodUnpackResult{
		Objects:     objects,
		ImageDigest: bundleImageDigest,
		Annotations: annotations,
	}, nil
}

func HandleUnexpectedPod(ctx context.Context, c client.Client, u *updater.Updater, pod *corev1.Pod) error {
	err := fmt.Errorf("unexpected pod phase: %v", pod.Status.Phase)
	_ = c.Delete(ctx, pod)
	return updater.UpdateStatusUnpackFailing(u, err)
}

func HandlePendingPod(u *updater.Updater, pod *corev1.Pod) {
	var messages []string
	for _, cStatus := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if cStatus.State.Waiting != nil && cStatus.State.Waiting.Reason == "ErrImagePull" {
			messages = append(messages, cStatus.State.Waiting.Message)
		}
		if cStatus.State.Waiting != nil && cStatus.State.Waiting.Reason == "ImagePullBackoff" {
			messages = append(messages, cStatus.State.Waiting.Message)
		}
	}
	u.UpdateStatus(
		updater.SetBundleInfo(nil),
		updater.EnsureBundleDigest(""),
		updater.SetPhase(olmv1alpha1.PhasePending),
		updater.EnsureCondition(metav1.Condition{
			Type:    olmv1alpha1.TypeUnpacked,
			Status:  metav1.ConditionFalse,
			Reason:  olmv1alpha1.ReasonUnpackPending,
			Message: strings.Join(messages, "; "),
		}),
	)
}

func HandleRunningPod(u *updater.Updater) {
	u.UpdateStatus(
		updater.SetBundleInfo(nil),
		updater.EnsureBundleDigest(""),
		updater.SetPhase(olmv1alpha1.PhaseUnpacking),
		updater.EnsureCondition(metav1.Condition{
			Type:   olmv1alpha1.TypeUnpacked,
			Status: metav1.ConditionFalse,
			Reason: olmv1alpha1.ReasonUnpacking,
		}),
	)
}

func HandleFailedPod(ctx context.Context, c client.Client, kc kubernetes.Interface, u *updater.Updater, pod *corev1.Pod) error {
	u.UpdateStatus(
		updater.SetBundleInfo(nil),
		updater.EnsureBundleDigest(""),
		updater.SetPhase(olmv1alpha1.PhaseFailing),
	)
	logs, err := GetPodLogs(ctx, kc, pod)
	if err != nil {
		err = fmt.Errorf("unpack failed: failed to retrieve failed pod logs: %w", err)
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return err
	}
	logStr := string(logs)
	u.UpdateStatus(
		updater.EnsureCondition(metav1.Condition{
			Type:    olmv1alpha1.TypeUnpacked,
			Status:  metav1.ConditionFalse,
			Reason:  olmv1alpha1.ReasonUnpackFailed,
			Message: logStr,
		}),
	)
	_ = c.Delete(ctx, pod)
	return fmt.Errorf("unpack failed: %v", logStr)
}

func GetPodLogs(ctx context.Context, kubeClient kubernetes.Interface, pod *corev1.Pod) ([]byte, error) {
	logReader, err := kubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pod logs: %w", err)
	}
	defer logReader.Close()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, logReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func getBundleImageDigest(pod *corev1.Pod) (string, error) {
	for _, ps := range pod.Status.InitContainerStatuses {
		if ps.Name == "copy-bundle" && ps.ImageID != "" {
			return ps.ImageID, nil
		}
	}
	return "", fmt.Errorf("bundle image digest not found")
}

func getAnnotations(bundleFS fs.FS) (*registry.Annotations, error) {
	fileData, err := fs.ReadFile(bundleFS, filepath.Join("metadata", "annotations.yaml"))
	if err != nil {
		return nil, err
	}
	annotationsFile := registry.AnnotationsFile{}
	if err := yaml.Unmarshal(fileData, &annotationsFile); err != nil {
		return nil, err
	}
	return &annotationsFile.Annotations, nil
}

func getObjects(bundleFS fs.FS) ([]client.Object, error) {
	var objects []client.Object
	const manifestsDir = "manifests"

	entries, err := fs.ReadDir(bundleFS, manifestsDir)
	if err != nil {
		return nil, fmt.Errorf("read manifests: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fileData, err := fs.ReadFile(bundleFS, filepath.Join(manifestsDir, e.Name()))
		if err != nil {
			return nil, err
		}

		dec := apimachyaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileData), 1024)
		for {
			obj := unstructured.Unstructured{}
			err := dec.Decode(&obj)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			objects = append(objects, &obj)
		}
	}
	return objects, nil
}

func getBundleContents(ctx context.Context, contents []byte) (fs.FS, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	bundleContents := map[string][]byte{}
	if err := decoder.Decode(&bundleContents); err != nil {
		return nil, err
	}
	bundleFS := fstest.MapFS{}
	for name, data := range bundleContents {
		bundleFS[name] = &fstest.MapFile{Data: data}
	}
	return bundleFS, nil
}
