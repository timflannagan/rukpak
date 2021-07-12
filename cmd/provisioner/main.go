package main

import (
	"flag"
	"os"

	"github.com/operator-framework/rukpak/pkg/k8s/provisioner"
	"k8s.io/apimachinery/pkg/runtime"
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

const (
	// TODO: add this as a flag that defaults to the upstream namespace
	globalNamespace = "rukpak"
	unpackImage     = "quay.io/tflannag/manifest:unpacker"
)

// TODO: handle adding a generateName for the bundle unpacking to avoid ifalreadyexists errors during the create call?
func main() {
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		setupLog.Error(err, "failed to add a new scheme")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), manager.Options{
		Scheme:    scheme,
		Namespace: globalNamespace,
	})
	if err != nil {
		setupLog.Error(err, "failed to setup manager instance")
		os.Exit(1)
	}

	r, err := provisioner.NewReconciler(
		mgr.GetClient(),
		setupLog,
		scheme,
		globalNamespace,
		unpackImage,
	)
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
	setupLog.Info("exiting manager")
}
