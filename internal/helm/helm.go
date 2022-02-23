package helm

import (
	"errors"

	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type releaseState string

const (
	StateNeedsInstall releaseState = "NeedsInstall"
	StateNeedsUpgrade releaseState = "NeedsUpgrade"
	StateUnchanged    releaseState = "Unchanged"
	StateError        releaseState = "Error"
)

func GetReleaseState(cl helmclient.ActionInterface, obj metav1.Object, chrt *chart.Chart, namespace string) (*release.Release, releaseState, error) {
	currentRelease, err := cl.Get(obj.GetName())
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, StateError, err
	}
	if errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, StateNeedsInstall, nil
	}
	desiredRelease, err := cl.Upgrade(obj.GetName(), namespace, chrt, nil, func(upgrade *action.Upgrade) error {
		upgrade.DryRun = true
		return nil
	})
	if err != nil {
		return currentRelease, StateError, err
	}
	if desiredRelease.Manifest != currentRelease.Manifest ||
		currentRelease.Info.Status == release.StatusFailed ||
		currentRelease.Info.Status == release.StatusSuperseded {
		return currentRelease, StateNeedsUpgrade, nil
	}
	return currentRelease, StateNeedsUpgrade, nil
}
