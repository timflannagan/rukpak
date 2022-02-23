/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	v1 "github.com/operator-framework/api/pkg/operators/v1"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"

	olmv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/convert"
	"github.com/operator-framework/rukpak/internal/helm"
	helmpredicate "github.com/operator-framework/rukpak/internal/helm-operator-plugins/predicate"
	"github.com/operator-framework/rukpak/internal/storage"
	"github.com/operator-framework/rukpak/internal/util"
)

// BundleInstanceReconciler reconciles a BundleInstance object
type BundleInstanceReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Controller controller.Controller

	ActionClientGetter helmclient.ActionClientGetter
	BundleStorage      storage.Storage
	ReleaseNamespace   string

	dynamicWatchMutex sync.RWMutex
	dynamicWatchGVKs  map[schema.GroupVersionKind]struct{}
}

//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=operators.coreos.com,resources=operatorgroups,verbs=get;list;watch
//+kubebuilder:rbac:groups=*,resources=*,verbs=*

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the BundleInstance object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *BundleInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	l.V(1).Info("starting reconciliation")
	defer l.V(1).Info("ending reconciliation")

	bi := &olmv1alpha1.BundleInstance{}
	if err := r.Get(ctx, req.NamespacedName, bi); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	defer func() {
		bi := bi.DeepCopy()
		bi.ObjectMeta.ManagedFields = nil
		if err := r.Status().Patch(ctx, bi, client.Apply, client.FieldOwner("kuberpak.io/registry+v1")); err != nil {
			l.Error(err, "failed to patch status")
		}
	}()

	b := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: bi.Spec.BundleName}, b); err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "BundleLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	reg, err := r.loadBundle(ctx, bi)
	if err != nil {
		var bnuErr *errBundleNotUnpacked
		if errors.As(err, &bnuErr) {
			reason := fmt.Sprintf("BundleUnpack%s", b.Status.Phase)
			if b.Status.Phase == olmv1alpha1.PhaseUnpacking {
				reason = "BundleUnpackRunning"
			}
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:   "Installed",
				Status: metav1.ConditionFalse,
				Reason: reason,
			})
			return ctrl.Result{}, nil
		}
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "BundleLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	installNamespace := fmt.Sprintf("%s-system", b.Status.Info.Package)
	if ns, ok := bi.Annotations["kuberpak.io/install-namespace"]; ok && ns != "" {
		installNamespace = ns
	} else if ns, ok := reg.CSV.Annotations["operatorframework.io/suggested-namespace"]; ok && ns != "" {
		installNamespace = ns
	}

	og, err := r.getOperatorGroup(ctx, installNamespace)
	if err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "OperatorGroupLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}
	if og.Status.LastUpdated == nil {
		err := errors.New("target naemspaces unknown")
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "OperatorGroupNotReady",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}
	desiredObjects, err := convert.GetDesiredObjects(*reg, installNamespace, og.Status.Namespaces)
	if err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "BundleLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	chrt := &chart.Chart{
		Metadata: &chart.Metadata{},
	}
	for _, obj := range desiredObjects {
		jsonData, err := yaml.Marshal(obj)
		if err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "BundleLookupFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
		hash := sha256.Sum256(jsonData)
		chrt.Templates = append(chrt.Templates, &chart.File{
			Name: fmt.Sprintf("object-%x.yaml", hash[0:8]),
			Data: jsonData,
		})
	}

	bi.SetNamespace(r.ReleaseNamespace)
	cl, err := r.ActionClientGetter.ActionClientFor(bi)
	bi.SetNamespace("")
	if err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "ErrorGettingClient",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	rel, state, err := helm.GetReleaseState(cl, bi, chrt, r.ReleaseNamespace)
	if err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "ErrorGettingReleaseState",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	switch state {
	case helm.StateNeedsInstall:
		_, err = cl.Install(bi.Name, r.ReleaseNamespace, chrt, nil, func(install *action.Install) error {
			install.CreateNamespace = false
			return nil
		})
		if err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "InstallFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	case helm.StateNeedsUpgrade:
		_, err = cl.Upgrade(bi.Name, r.ReleaseNamespace, chrt, nil)
		if err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "UpgradeFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	case helm.StateUnchanged:
		if err := cl.Reconcile(rel); err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "ReconcileFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	default:
		return ctrl.Result{}, fmt.Errorf("unexpected release state %q", state)
	}

	for _, obj := range desiredObjects {
		uMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "CreateDynamicWatchFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}

		u := &unstructured.Unstructured{Object: uMap}
		if err := func() error {
			r.dynamicWatchMutex.Lock()
			defer r.dynamicWatchMutex.Unlock()

			_, isWatched := r.dynamicWatchGVKs[u.GroupVersionKind()]
			if !isWatched {
				if err := r.Controller.Watch(
					&source.Kind{Type: u},
					&handler.EnqueueRequestForOwner{OwnerType: bi, IsController: true},
					helmpredicate.DependentPredicateFuncs()); err != nil {
					return err
				}
				r.dynamicWatchGVKs[u.GroupVersionKind()] = struct{}{}
			}
			return nil
		}(); err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "CreateDynamicWatchFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	}
	meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
		Type:   "Installed",
		Status: metav1.ConditionTrue,
		Reason: "InstallationSucceeded",
	})
	bi.Status.InstalledBundleName = bi.Spec.BundleName
	return ctrl.Result{}, nil
}

func (r *BundleInstanceReconciler) getOperatorGroup(ctx context.Context, installNamespace string) (*v1.OperatorGroup, error) {
	ogs := v1.OperatorGroupList{}
	if err := r.List(ctx, &ogs, client.InNamespace(installNamespace)); err != nil {
		return nil, err
	}
	switch len(ogs.Items) {
	case 0:
		return nil, fmt.Errorf("no operator group found in install namespace %q", installNamespace)
	case 1:
		return &ogs.Items[0], nil
	default:
		return nil, fmt.Errorf("multiple operator groups found in install namespace")
	}
}

type errBundleNotUnpacked struct {
	currentPhase string
}

func (err errBundleNotUnpacked) Error() string {
	const baseError = "bundle is not yet unpacked"
	if err.currentPhase == "" {
		return baseError
	}
	return fmt.Sprintf("%s, current phase=%s", baseError, err.currentPhase)
}

func (r *BundleInstanceReconciler) loadBundle(ctx context.Context, bi *olmv1alpha1.BundleInstance) (*convert.RegistryV1, error) {
	b := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: bi.Spec.BundleName}, b); err != nil {
		return nil, fmt.Errorf("get bundle %q: %w", bi.Spec.BundleName, err)
	}
	if b.Status.Phase != olmv1alpha1.PhaseUnpacked {
		return nil, &errBundleNotUnpacked{currentPhase: b.Status.Phase}
	}

	objects, err := r.BundleStorage.Load(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("load bundle objects: %w", err)
	}

	reg := convert.RegistryV1{}
	for _, obj := range objects {
		obj := obj
		obj.SetLabels(map[string]string{
			"kuberpak.io/owner-name": bi.Name,
		})
		switch obj.GetObjectKind().GroupVersionKind().Kind {
		case "ClusterServiceVersion":
			csv := v1alpha1.ClusterServiceVersion{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &csv); err != nil {
				return nil, err
			}
			reg.CSV = csv
		case "CustomResourceDefinition":
			crd := apiextensionsv1.CustomResourceDefinition{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &crd); err != nil {
				return nil, err
			}
			reg.CRDs = append(reg.CRDs, crd)
		default:
			reg.Others = append(reg.Others, obj)
		}
	}
	return &reg, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//r.ActionConfigGetter = helmclient.NewActionConfigGetter(mgr.GetConfig(), mgr.GetRESTMapper(), mgr.GetLogger())
	//r.ActionClientGetter = helmclient.NewActionClientGetter(r.ActionConfigGetter)
	controller, err := ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.BundleInstance{}, builder.WithPredicates(util.BundleInstanceProvisionerFilter("kuberpak.io/registry+v1"))).
		Watches(&source.Kind{Type: &olmv1alpha1.Bundle{}}, handler.EnqueueRequestsFromMapFunc(util.MapBundleToBundleInstanceHandler(mgr.GetClient(), mgr.GetLogger()))).
		Build(r)
	if err != nil {
		return err
	}
	r.Controller = controller
	r.dynamicWatchGVKs = map[schema.GroupVersionKind]struct{}{}
	return nil
}
