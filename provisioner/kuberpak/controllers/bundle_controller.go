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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	olmv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/storage"
	"github.com/operator-framework/rukpak/internal/unpacker"
	"github.com/operator-framework/rukpak/internal/updater"
)

// BundleReconciler reconciles a Bundle object
type BundleReconciler struct {
	client.Client
	KubeClient kubernetes.Interface
	Scheme     *runtime.Scheme
	Storage    storage.Storage
	Unpacker   unpacker.Unpacker

	PodNamespace string
}

//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=pods;secrets;configmaps,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=core,resources=pods/log,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *BundleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reconcileErr error) {
	l := log.FromContext(ctx)
	l.V(1).Info("starting reconciliation")
	defer l.V(1).Info("ending reconciliation")
	bundle := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	u := updater.New(r.Client)
	defer func() {
		if err := u.Apply(ctx, bundle); err != nil {
			l.Error(err, "failed to update status")
		}
	}()
	u.UpdateStatus(updater.EnsureObservedGeneration(bundle.Generation))

	res, err := r.Unpacker.UnpackBundle(ctx, bundle)
	if err != nil {
		u.UpdateStatus(updater.SetBundleInfo(nil), updater.EnsureBundleDigest(""))
		return ctrl.Result{}, updater.UpdateStatusUnpackFailing(&u, fmt.Errorf("ensure unpack pod: %w", err))
	}
	if res == nil {
		return ctrl.Result{}, updater.UpdateStatusUnpackFailing(&u, fmt.Errorf("empty unpack result"))
	}

	if err := r.Storage.Store(ctx, bundle, res.Objects); err != nil {
		return ctrl.Result{}, updater.UpdateStatusUnpackFailing(&u, fmt.Errorf("persist bundle objects: %w", err))
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.Bundle{}, builder.WithPredicates(
			bundleProvisionerFilter("kuberpak.io/registry+v1"),
		)).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func bundleProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.Bundle)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}
