package main

import (
	"flag"
	"os"

	"github.com/operator-framework/rukpak/pkg/k8s/provisioner"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), manager.Options{
		Scheme:    scheme,
		Namespace: "openshift-operator-lifecycle-manager",
	})
	if err != nil {
		setupLog.Error(err, "failed to setup manager instance")
		os.Exit(1)
	}

	r, err := provisioner.NewReconciler(mgr.GetClient(), setupLog, scheme)
	if err != nil {
		setupLog.Error(err, "failed to create a new reconciler")
		os.Exit(1)
	}

	if err := r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "failed to attach the provisioner controllers to the manager instance")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder
	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
