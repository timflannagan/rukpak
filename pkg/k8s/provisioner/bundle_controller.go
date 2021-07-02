package provisioner

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/pkg/k8s/manifests"
)

var (
	localSchemeBuilder = runtime.NewSchemeBuilder(
		kscheme.AddToScheme,
		v1alpha1.AddToScheme,
	)

	// AddToScheme adds all types necessary for the controller to operate.
	AddToScheme = localSchemeBuilder.AddToScheme
)

const (
	ID       v1alpha1.ProvisionerID = "operators.coreos.com/olm"
	opmImage                        = "quay.io/operator-framework/upstream-opm-builder:v1.17.4"
)

type Reconciler struct {
	client.Client
	log logr.Logger
}

// NewReconciler constructs and returns an BundleReconciler.
// As a side effect, the given scheme has operator discovery types added to it.
func NewReconciler(cli client.Client, log logr.Logger, scheme *runtime.Scheme) (*Reconciler, error) {
	// Add watched types to scheme.
	if err := AddToScheme(scheme); err != nil {
		return nil, err
	}

	return &Reconciler{
		Client: cli,
		log:    log,
	}, nil
}

// +kubebuilder:rbac:groups=core.rukpak.io,resources=provisionerclasses,verbs=create;update;patch;delete
// +kubebuilder:rbac:groups=core.rukpak.io,resources=bundles,verbs=create;update;patch;delete
// +kubebuilder:rbac:groups=core.rukpak.io,resources=bundles/status,verbs=update;patch
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch

// SetupWithManager adds the operator reconciler to the given controller manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Bundle{}).
		Complete(reconcile.Func(r.ReconcileBundle))
	if err != nil {
		return err
	}

	predicateProvisionerIDFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		pc, ok := obj.(*v1alpha1.ProvisionerClass)
		if !ok {
			return false
		}
		return pc.Spec.Provisioner == ID
	})
	err = ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ProvisionerClass{}, builder.WithPredicates(predicateProvisionerIDFilter)).
		Complete(reconcile.Func(r.ReconcileProvisionerClass))
	if err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) ReconcileBundle(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	log := r.log.WithValues("request", req)
	log.V(1).Info("reconciling bundle")

	bundle := &v1alpha1.Bundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		log.Error(err, "errors requesting bundle custom resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	/*
		- User creates a Bundle custom resource
		- A ProvisionerClass resource ensures there's a volume creates for that Bundle resource and updates the status sub-resource field
		- A Bundle resource will then ensure a Job is created that's responsible for unpacking those resources to a volume created by the Provisioner?

		Bundle -> Provisioner creates volume -> Bundle unpacks content to that volume -> a filesystem HTTP API is created?
	*/

	bundleType := bundle.Spec.Source.Ref
	if !strings.Contains(bundleType, "docker") {
		log.Info("bundle", "non-docker bundle types are not supported yet", bundleType)
		return ctrl.Result{}, nil
	}
	if bundle.Status.Volume == nil {
		log.Info("bundle", "waiting for the provisioner to create a volume for the bundle", bundle.Name)
		return ctrl.Result{}, nil
	}

	if err := r.normalBundleUnpackJob(ctx, log, bundle); err != nil {
		log.Error(err, "failed to create a bundle unpack job")
		return ctrl.Result{}, err
	}

	// Want:
	// Content oracle -- Content(bundle.Spec.Source) io.Reader
	// Open Question:
	// 1. Check bundle state
	// - look for unpack results
	// - is there anything missing
	// - does the digest match
	// 2. Unpack bundle (if not) -- Asynchronous (maybe a Job)?
	// - take the bundlesource
	// - fire job based on type to unpack
	// 3. Update status

	return reconcile.Result{}, nil
}

func (r *Reconciler) normalBundleUnpackJob(ctx context.Context, log logr.Logger, bundle *v1alpha1.Bundle) error {
	job := &batchv1.Job{}
	jobNN := types.NamespacedName{
		Name:      bundle.Name,
		Namespace: "openshift-operator-lifecycle-manager",
	}

	bundleType := bundle.Spec.Source.Ref
	err := r.Client.Get(ctx, jobNN, job)
	if err != nil {
		log.Info("job already exists -- not updating (stub)")
		return nil
	}
	if apierrors.IsNotFound(err) {
		config := manifests.BundleUnpackJobConfig{
			JobName:             bundle.Name,
			BundleImage:         strings.Replace(bundleType, "docker://", "", -1),
			BundleConfigMapName: bundle.Status.Volume.Name,
		}
		log.Info("job", "rendering the Job Template", config)
		data, err := manifests.NewJobTemplate(config)
		if err != nil {
			log.Error(err, "failed to instantiate a new job template")
			return err
		}

		log.Info("serializing the rendered job template")
		job, err := manifests.NewJobManifest(data)
		if err != nil {
			log.Error(err, "failed to serialize the job manifest from rendered template")
			return err
		}
		log.Info("job", "configuration", job)

		log.Info("job", "attempting to create the bundle unpack job", job.Name)
		if err := r.Client.Create(ctx, job); err != nil {
			log.Error(err, "failed to create the bundle unpack job")
			return err
		}
		log.Info("job", "bundle unpack job created successfully", job.Name)
	}
	return nil
}

func (r *Reconciler) ReconcileProvisionerClass(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	// Set up a convenient log object so we don't have to type request over and over again
	log := r.log.WithValues("request", req)
	log.V(1).Info("reconciling provisionerclass")

	// Are provisionerclass(es) namespaced-scoped resources?
	pc := &v1alpha1.ProvisionerClass{}
	if err := r.Client.Get(ctx, req.NamespacedName, pc); err != nil {
		log.Error(err, "failed to query for the ProvisionerClass")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// we know we're only handling ProvisionerClass' that use the OLM provisioner ID
	// need to configure this controller based on the parameters passed through here

	// list bundles that match the current ProvisionerClass' metadata.Name

	bundles := &v1alpha1.BundleList{}
	if err := r.Client.List(ctx, bundles, client.InNamespace(req.Namespace)); err != nil {
		log.Error(err, "failed to list all the bundles in the req.Namespace")
		return ctrl.Result{}, err
	}

	filtered := []*v1alpha1.Bundle{}
	for _, b := range bundles.Items {
		if string(b.Spec.Class) != pc.Name {
			continue
		}
		filtered = append(filtered, &b)
	}

	// TODO(tflannag): Wrap this in a helper function
	var errors []error
	for _, b := range filtered {
		if b.Status.Volume != nil {
			// TODO(tflannag): Verify the status
			continue
		}

		bundle := &v1alpha1.Bundle{}
		if err := r.Client.Get(ctx, types.NamespacedName{Name: b.Name}, bundle); err != nil {
			log.Error(err, "failed to get a bundle")
			errors = append(errors, fmt.Errorf("failed to get a non-empty configmap"))
			continue
		}

		log.Info("volume", "identified bundle missing configmap", bundle.Name)
		cm, err := newConfigMap(r.Client, types.NamespacedName{Name: bundle.Name, Namespace: "openshift-operator-lifecycle-manager"})
		if err != nil {
			log.Error(err, "failed to get or create a fresh configmap")
			errors = append(errors, err)
			continue
		}
		if cm == nil {
			errors = append(errors, fmt.Errorf("failed to get a non-empty configmap"))
			continue
		}

		needsUpdate := false
		if bundle.Status.Volume != nil {
			log.Info("volume", "bundle contains an existing local object reference", bundle.Name, "checking if it matches an existing configmap")
			if bundle.Status.Volume.Name == cm.Name {
				continue
			}
			needsUpdate = true
			log.Info("volume", "bundle contains an existing local object reference", bundle.Name, "does not match the configmap updating")
		}
		if bundle.Status.Volume == nil {
			needsUpdate = true
			log.Info("volume", "bundle does not contain a local object reference", bundle.Name, "updating it to use a configmap", cm.Name)
		}
		if !needsUpdate {
			log.Info("volume", "not further updates are required for bundle", bundle.Name)
			continue
		}

		log.Info("volume", "status updates are required for bundle", bundle.Name)
		bundle.Status.Volume = &corev1.LocalObjectReference{
			Name: cm.Name,
		}
		log.Info("volume", "attempting to update the bundle status", bundle.Name, "new bundle", bundle.Status)
		if err := r.Status().Update(ctx, bundle); err != nil {
			log.Error(err, "failed to update the bundle status", bundle.Name)
			return ctrl.Result{}, err
		}
		log.Info("volume", "updated the bundle status", bundle.Name)
	}
	if len(errors) != 0 {
		return ctrl.Result{}, utilerrors.NewAggregate(errors)
	}

	return reconcile.Result{}, nil
}

func get

// newConfigMap is a helper function that's responsible for building
// up an empty ConfigMap that contains the requisite metadata and
// ensures that ConfigMap resource is created if it doesn't exist.
func newConfigMap(client client.Client, nn types.NamespacedName) (*corev1.ConfigMap, error) {
	// need to set an owner reference
	fresh := &corev1.ConfigMap{}
	fresh.SetNamespace(nn.Namespace)
	fresh.SetName(nn.Name)

	var configmap corev1.ConfigMap
	err := client.Get(context.TODO(), nn, &configmap)
	if apierrors.IsNotFound(err) {
		if err := client.Create(context.TODO(), fresh); err != nil {
			return nil, err
		}
		return fresh, nil
	}

	return fresh, err
}
