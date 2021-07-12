package provisioner

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/pkg/k8s/manifests"
)

// TODO(tlannag): Need to expose a way to configure the namespace the bundle volume content is being created in. A global namespace?
// TODO(tflannag): Requeue after a period for bundles w/o a volume already present
// TODO(tflannag): Flesh out local volume logic
// TODO(tflannag): Add support for authentication
// TODO(tflannag): Investigate a better way to expose this to the unpackBundle method
// TODO(tflannag): Likely need to abstract this type of bundle unpacking logic further
// TODO(tflannag): Unpack to a location instead of streaming to stdout
// TODO(tflannag): Avoid hardcoding this -- either expose a directory or unpack _everything_
// TODO(tflannag): Determine if there's a better way of filtering out bundles that contain the current ProvisionerClass metadata.NAme
// TODO(tflannag): Need to configure this controller based on the parameters passed through here
// TODO(tflannag): Likely want to be able to configure a ProvisionerClass to support
// 				   a series of losely defined parameters, like use an existing volume for this bundle content
// 		           or use this type of persistentvolume when attaching storage to a bundle content filesystem
// TODO(tflannag): Add update support for configmap volumes
// TODO(tflannag): Only filter for bundles that has something like a rukpak provisioner annotation present?
// TODO(tflannag): Flesh out bundle updates, e.g. react to changes to something like an image tag changing

var (
	localSchemeBuilder = runtime.NewSchemeBuilder(
		kscheme.AddToScheme,
		v1alpha1.AddToScheme,
	)
	// AddToScheme adds all types necessary for the controller to operate.
	AddToScheme = localSchemeBuilder.AddToScheme
)

const (
	// ID is the rukpak provisioner's unique ID. Only ProvisionerClass(es) that specify
	// this unique ID will be managed by this provisioner controller.
	ID v1alpha1.ProvisionerID = "rukpack.io/k8s"
)

type Reconciler struct {
	client.Client
	log             logr.Logger
	globalNamespace string
}

// NewReconciler constructs and returns an BundleReconciler.
// As a side effect, the given scheme has operator discovery types added to it.
// TODO: implement the options pattern instead - use custom type instead of a long list of parameters?
func NewReconciler(
	cli client.Client,
	log logr.Logger,
	scheme *runtime.Scheme,
	globalNamespace string,
) (*Reconciler, error) {
	// Add watched types to scheme.
	if err := AddToScheme(scheme); err != nil {
		return nil, err
	}

	return &Reconciler{
		Client:          cli,
		log:             log,
		globalNamespace: globalNamespace,
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
		Watches(&source.Kind{Type: &v1alpha1.Bundle{}}, handler.EnqueueRequestsFromMapFunc(r.bundleHandler)).
		Complete(reconcile.Func(r.ReconcileProvisionerClass))
	if err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) bundleHandler(obj client.Object) []reconcile.Request {
	log := r.log.WithValues("bundle", obj.GetName())

	nn := types.NamespacedName{Name: obj.GetName()}
	bundle := &v1alpha1.Bundle{}
	if err := r.Client.Get(context.TODO(), nn, bundle); err != nil {
		return []reconcile.Request{}
	}

	provisioners := &v1alpha1.ProvisionerClassList{}
	if err := r.Client.List(context.TODO(), provisioners); err != nil {
		return []reconcile.Request{}
	}
	if len(provisioners.Items) == 0 {
		return []reconcile.Request{}
	}

	res := []reconcile.Request{}
	for _, provisioner := range provisioners.Items {
		if provisioner.GetName() != string(bundle.Spec.Class) {
			continue
		}
		// TODO(tflannag): Should the ProvisionerClass be namespaced-scoped?
		res = append(res, reconcile.Request{NamespacedName: types.NamespacedName{Name: provisioner.GetName()}})
	}
	if len(res) == 0 {
		log.Info("no provisionerclass(es) need to be requeued after encountering a bundle event")
		return []reconcile.Request{}
	}

	log.Info("handler", "requeueing provisionerclass(es) after encountering a bundle event", nn.Name)
	return res
}

// ReconcileBundle contains the main reconciliation logic for ensuring that arbitrary content
// specified in Bundle custom resources are unpacked and exposed to clients to be inspected,
// installed, etc.
func (r *Reconciler) ReconcileBundle(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	log := r.log.WithValues("request", req)
	log.V(1).Info("reconciling bundle")

	bundle := &v1alpha1.Bundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	bundleType := bundle.Spec.Source.Ref
	if !strings.Contains(bundleType, "docker") {
		log.Info("bundle", "non-docker bundle types are not supported yet", bundleType)
		return ctrl.Result{}, nil
	}
	if bundle.Status.Volume == nil {
		log.Info("bundle", "waiting for the provisioner to create a volume for the bundle", bundle.Name)
		return ctrl.Result{}, nil
	}

	if !strings.Contains(string(bundle.Spec.Source.Ref), "docker://") {
		log.Info("bundle", "cannot process non-docker bundle sources", bundle.Name, "source", bundle.Spec.Class)
		return ctrl.Result{}, nil
	}

	if err := r.unpackBundle(bundle); err != nil {
		log.Error(err, "failed to unpack bunde")
		return ctrl.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *Reconciler) unpackBundle(bundle *v1alpha1.Bundle) error {
	// TODO(tflannag): Likely want to have this logic live inside of a pod
	// so we can mount volumes and treat everything as a filesystem.
	bundleSource := bundle.Spec.Source.Ref
	if strings.Contains(bundleSource, "docker://") {
		bundleSource = strings.TrimPrefix(bundleSource, "docker://")
	}

	if err := r.newUnpackJob(bundle.Status.Volume.Name, "quay.io/tflannag/manifest:unpacker"); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) newUnpackJob(pvcName, image string) error {
	// setup ttl delete
	config := manifests.BundleUnpackJobConfig{
		JobName:      "tmp",
		JobNamespace: r.globalNamespace,
		BundleImage:  image,
		PVCName:      pvcName,
	}
	data, err := manifests.NewJobTemplate(config)
	if err != nil {
		return err
	}
	job, err := manifests.NewJobManifest(data)
	if err != nil {
		return err
	}
	if err := r.Client.Create(context.Background(), job); err != nil {
		return client.IgnoreNotFound(err)
	}

	return nil
}

// ReconcileProvisionerClass is responsible for ensuring the specification defined in
// an individual ProvisionerClass custom resources matches the on-cluster state of
// the resource. A Bundle custom resource referencing an individual ProvisionerClass'
// metadata.Name will be managed by the reconciliation loop, ensuring that a volume
// exists such that arbitrary content specified in a Bundle custom resource and be
// available for inspection to end-users.
func (r *Reconciler) ReconcileProvisionerClass(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	// Set up a convenient log object so we don't have to type request over and over again
	log := r.log.WithValues("request", req)
	log.V(1).Info("reconciling provisionerclass")

	pc := &v1alpha1.ProvisionerClass{}
	if err := r.Client.Get(ctx, req.NamespacedName, pc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	bundles := &v1alpha1.BundleList{}
	if err := r.Client.List(ctx, bundles, client.InNamespace(req.Namespace)); err != nil {
		log.Error(err, "failed to list all the bundles in the req.Namespace")
		return ctrl.Result{}, err
	}

	filtered := []*v1alpha1.Bundle{}
	for _, b := range bundles.Items {
		if string(b.Spec.Class) != pc.Name {
			log.Info("provisioner", "found bundle name that does not reference the current provisioner class", b.Name)
			continue
		}
		filtered = append(filtered, &b)
	}
	if len(filtered) == 0 {
		log.Info("no bundles found specifying the current provisionerclass")
		return ctrl.Result{}, nil
	}

	var errors []error
	for _, bundle := range filtered {
		log.Info("provisioner", "found bundle name that references the current provisionerclass", bundle.Name)
		if err := r.ensureBundleVolume(ctx, bundle.GetName()); err != nil {
			errors = append(errors, err)
		}
	}

	return ctrl.Result{}, utilerrors.NewAggregate(errors)
}

// ensureBundleVolume is a helper method responsible for ensuring the bundle's status
// references an existing volume that will the content stored in the bundle sources'
// filesystem.
func (r *Reconciler) ensureBundleVolume(ctx context.Context, bundleName string) error {
	nn := types.NamespacedName{Name: bundleName}
	fresh := &v1alpha1.Bundle{}

	if err := r.Client.Get(ctx, nn, fresh); err != nil {
		return err
	}

	if fresh.Status.Volume != nil {
		// FIXME
		r.log.Info("volume", "(stub) bundle volume already exists not validating yet", fresh.Name)
		return nil
	}

	pv, err := r.createPV(fresh.GetName())
	if err != nil {
		return err
	}
	fresh.Status.Volume = &corev1.LocalObjectReference{
		Name: pv.GetName(),
	}

	r.log.Info("volume", "attempting to update the bundle volume", fresh.GetName(), "with pv name", pv.GetName())
	if err := r.Client.Status().Update(ctx, fresh); err != nil {
		return err
	}
	r.log.Info("volume", "bundle status has been updated to point to a volume created", fresh.GetName())

	return nil
}

func (r *Reconciler) createPV(bundleName string) (*corev1.PersistentVolume, error) {
	pvName := fmt.Sprintf("%s-pvc", bundleName)
	nn := types.NamespacedName{Name: pvName, Namespace: r.globalNamespace}
	pv := &corev1.PersistentVolume{}
	ctx := context.Background()

	if err := r.Client.Get(ctx, nn, pv); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}

		pv.SetName(pvName)
		pv.SetNamespace(r.globalNamespace)
		volumeMode := corev1.PersistentVolumeFilesystem
		pv.Spec = corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  &volumeMode,
			Capacity: corev1.ResourceList{
				corev1.ResourceName(corev1.ResourceStorage): apiresource.MustParse("2Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/manifests",
				},
			},
		}
		r.log.Info("provisioner", "attempting to create a pv for the bundle name", bundleName)
		if err := r.Client.Create(ctx, pv); err != nil {
			return nil, err
		}
		r.log.Info("provisioner", "created a pv for the bundle name", bundleName)
	}

	return pv, nil
}
