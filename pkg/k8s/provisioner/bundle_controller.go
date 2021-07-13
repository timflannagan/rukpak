package provisioner

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	unpackImage     string
}

// NewReconciler constructs and returns an BundleReconciler.
// As a side effect, the given scheme has operator discovery types added to it.
// TODO: implement the options pattern instead - use custom type instead of a long list of parameters?
func NewReconciler(
	cli client.Client,
	log logr.Logger,
	scheme *runtime.Scheme,
	globalNamespace,
	unpackImage string,
) (*Reconciler, error) {
	// Add watched types to scheme.
	if err := AddToScheme(scheme); err != nil {
		return nil, err
	}

	return &Reconciler{
		Client:          cli,
		log:             log,
		globalNamespace: globalNamespace,
		unpackImage:     unpackImage,
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

	bundle := &v1alpha1.Bundle{}
	if err := r.Client.Get(context.TODO(), getNonNamespacedName(obj.GetName()), bundle); err != nil {
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
		res = append(res, reconcile.Request{NamespacedName: getNonNamespacedName(provisioner.GetName())})
	}
	if len(res) == 0 {
		log.Info("no provisionerclass(es) need to be requeued after encountering a bundle event")
		return []reconcile.Request{}
	}

	log.Info("handler", "requeueing provisionerclass(es) after encountering a bundle event", obj.GetName())
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

	// TODO(tflannag): Should shard the filesystem content better so it's namespaced
	// at least by bundle name
	// TODO(tflannag): Need to wait until the job that's unpacking contents has become
	// ready before serving filesystem content.
	// TODO(tflannag): Need a way to rotate the serving Pod when changes have been made
	// to the underlying sub-directory filesystem.
	if err := r.unpackBundle(bundle); err != nil {
		log.Error(err, "failed to unpack bunde")
		return ctrl.Result{}, err
	}
	if err := r.ensureManifestService(bundle.Name, 8081); err != nil {
		log.Error(err, "failed to ensure the manifest serving service exists")
		return ctrl.Result{}, err
	}
	// TODO(tflannag): The --directory option doesn't seem to play well with the mountPath provided
	if err := r.createManifestServingPod(bundle.Name, bundle.Status.Volume.Name, "/manifests"); err != nil {
		log.Error(err, "failed to create a manifest filesystem serving pod")
		return ctrl.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *Reconciler) ensureManifestService(name string, port int32) error {
	r.log.Info("serve", "ensuring a service exists for serving bundle manifest content", name)

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-service", name),
			Namespace: r.globalNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name: "serving",
				Port: port,
			}},
			Selector: map[string]string{
				// TODO(tflannag): Use a better label in the Pod manifest template
				"name": name,
			},
		},
	}
	err := r.Client.Create(context.Background(), service)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	// TODO: reconcile state as needed

	return nil
}

func (r *Reconciler) createManifestServingPod(name, pvcName, mountpath string) error {
	r.log.Info("serve", "creating a pod that serves the manifest bundle content", name, "using pvc name", pvcName, "and mountpath", mountpath)
	config := manifests.ManifestServingPod{
		PodName:      fmt.Sprintf("%s-serve", name),
		PodNamespace: r.globalNamespace,
		ServeImage:   "quay.io/tflannag/manifests:servev2",
		PVCName:      pvcName,
		PVCMountPath: mountpath,
	}

	pod, err := manifests.NewManifestServingPod(config)
	if err != nil {
		return err
	}
	if err := r.Client.Create(context.Background(), pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	return nil
}

func (r *Reconciler) unpackBundle(bundle *v1alpha1.Bundle) error {
	bundleSource := bundle.Spec.Source.Ref
	if strings.Contains(bundleSource, "docker://") {
		bundleSource = strings.TrimPrefix(bundleSource, "docker://")
	}

	if err := r.newUnpackJob(bundle.Status.Volume.Name, bundle.Name, bundleSource); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) newUnpackJob(pvcName, name, image string) error {
	r.log.Info("creating job", "job namespace", r.globalNamespace, "job unpack image", r.unpackImage)

	// setup ttl delete
	jobName := fmt.Sprintf("%s-bundle-job", name)
	config := manifests.BundleUnpackJobConfig{
		JobName:      jobName,
		JobNamespace: r.globalNamespace,
		UnpackImage:  r.unpackImage,
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
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	return nil
}
