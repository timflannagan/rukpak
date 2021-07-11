package main

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func getKubernetesClient() (kubernetes.Interface, error) {
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)
	restconfig, err := kubeconfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the kubernetes client config: %v", err)
	}
	client, err := kubernetes.NewForConfig(restconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the kubernetes clientset: %v", err)
	}
	return client, nil
}

func main() {
	// Add flag handling
	// --unpackImage
	// --bundleImage
	// --namespace
	bundleSource := "quay.io/tflannag/bundle:scratch"
	logrus.Infof("attempting to pull the %s bundle image", bundleSource)

	reader, err := pullBundleSource(bundleSource)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	manifestDir := "manifests"
	t := tar.NewReader(reader)
	for true {
		header, err := t.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			logrus.Infof("failed to unpack bundle: %v", err)
			break
		}
		target := filepath.Clean(header.Name)
		if !strings.Contains(target, manifestDir) {
			continue
		}
		logrus.Infof("processing manifest file %s", target)

		switch header.Typeflag {
		case tar.TypeDir:
			logrus.Infof("attempting to mkdir the %s file if it doesn't already exist", header.Name)
			os.Mkdir(header.Name, 0755)

		case tar.TypeReg:
			outFile, err := os.Create(header.Name)
			if err != nil {
				logrus.Fatalf("ExtractTarGz: Create() failed: %s", err.Error())
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, t); err != nil {
				logrus.Fatalf("ExtractTarGz: Copy() failed: %s", err.Error())
			}
		}
	}

}

func pullBundleSource(bundleSource string) (io.ReadCloser, error) {
	ref, err := name.ParseReference(bundleSource)
	if err != nil {
		return nil, err
	}
	client, err := getKubernetesClient()
	if err != nil {
		return nil, err
	}
	secret, err := k8schain.New(context.Background(), client, k8schain.Options{
		Namespace:          "olm",
		ServiceAccountName: "admin",
		ImagePullSecrets:   []string{"regcred"},
	})
	if err != nil {
		return nil, err
	}

	auth := authn.NewMultiKeychain(secret)
	image, err := remote.Image(ref, remote.WithAuthFromKeychain(auth))
	if err != nil {
		return nil, err
	}

	return mutate.Extract(image), nil
}
