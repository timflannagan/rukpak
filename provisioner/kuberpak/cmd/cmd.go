package cmd

import (
	"os"

	v1 "github.com/operator-framework/api/pkg/operators/v1"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	olmv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/storage"
	"github.com/operator-framework/rukpak/internal/unpacker"
	"github.com/operator-framework/rukpak/internal/util"
	"github.com/operator-framework/rukpak/provisioner/kuberpak/controllers"
	"github.com/spf13/cobra"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
	utilruntime.Must(olmv1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func NewRunCmd() *cobra.Command {
	var (
		unpackImage          string
		enableLeaderElection bool
		metricsAddr          string
		probeAddr            string
	)
	cmd := &cobra.Command{
		Use:  "run",
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := ctrl.GetConfigOrDie()
			kubeClient, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				setupLog.Error(err, "unable to create kubernetes client")
				os.Exit(1)
			}
			// TODO(tflannag): Make this a constant and rename to be match the same core.rukpak.io domain name
			// - This is somewhat awkward right now as internal packages also reference this label
			dependentRequirement, err := labels.NewRequirement("kuberpak.io/owner-name", selection.Exists, nil)
			if err != nil {
				setupLog.Error(err, "unable to create dependent label selector for cache")
				os.Exit(1)
			}
			dependentSelector := labels.NewSelector().Add(*dependentRequirement)
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 scheme,
				MetricsBindAddress:     metricsAddr,
				Port:                   9443,
				HealthProbeBindAddress: probeAddr,
				LeaderElection:         enableLeaderElection,
				LeaderElectionID:       "510f803c.core.rukpak.io",
				NewCache: cache.BuilderWithOptions(cache.Options{
					SelectorsByObject: cache.SelectorsByObject{
						&olmv1alpha1.BundleInstance{}: {},
						&olmv1alpha1.Bundle{}:         {},
						&v1.OperatorGroup{}:           {},
					},
					DefaultSelector: cache.ObjectSelector{
						Label: dependentSelector,
					},
				}),
			})
			if err != nil {
				setupLog.Error(err, "unable to start manager")
				os.Exit(1)
			}

			ns := util.PodNamespace("kuberpak-system")

			// TODO(tflannag): This should be configurable through the CLI?
			bundleStorage := &storage.ConfigMaps{
				Client:     mgr.GetClient(),
				Namespace:  ns,
				NamePrefix: "bundle-",
			}
			// TODO(tflannag): This needs to be initialized in the main.go
			// Only problem is with injecting the updater.Updater as we create that here
			unpacker := &unpacker.PodBundleUnpacker{
				Client:       mgr.GetClient(),
				KubeClient:   kubeClient,
				PodNamespace: ns,
				UnpackImage:  unpackImage,
			}

			if err = (&controllers.BundleReconciler{
				Client:     mgr.GetClient(),
				KubeClient: kubeClient,
				Scheme:     mgr.GetScheme(),
				Storage:    bundleStorage,
				Unpacker:   unpacker,
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "Bundle")
				return err
			}

			// TODO(tflannag): Figure out the issue with helm history and existing resources
			// e.g. there's owner ref issues
			cfgGetter := helmclient.NewActionConfigGetter(mgr.GetConfig(), mgr.GetRESTMapper(), mgr.GetLogger())
			if err = (&controllers.BundleInstanceReconciler{
				Client:             mgr.GetClient(),
				Scheme:             mgr.GetScheme(),
				BundleStorage:      bundleStorage,
				ReleaseNamespace:   ns,
				ActionClientGetter: helmclient.NewActionClientGetter(cfgGetter),
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "BundleInstance")
				return err
			}
			//+kubebuilder:scaffold:builder

			if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up health check")
				return err
			}
			if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
				setupLog.Error(err, "unable to set up ready check")
				return err
			}

			setupLog.Info("starting manager")
			if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
				setupLog.Error(err, "problem running manager")
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&unpackImage, "unpack-image", "quay.io/joelanford/kuberpak-unpack:v0.1.0", "the container image for unpacking bundle images")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", ":8080", "the address the metric endpoint binds to.")
	cmd.Flags().StringVar(&probeAddr, "health-probe-addr", ":8081", "the address the probe endpoint binds to.")
	cmd.Flags().BoolVar(&enableLeaderElection, "enable-leader-election", true, "configures whether leader election is enabled")

	return cmd
}
