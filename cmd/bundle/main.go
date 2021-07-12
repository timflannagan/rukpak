package main

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func main() {
	cmd := newUnpackCmd()

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "encountered an error while unpacking the bundle container image: %v", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	image, err := cmd.Flags().GetString("image")
	if err != nil {
		return err
	}
	manifestDir, err := cmd.Flags().GetString("manifest-dir")
	if err != nil {
		return err
	}
	outputDir, err := cmd.Flags().GetString("output-dir")
	if err != nil {
		return err
	}
	logger := logrus.New()

	client, err := getKubernetesClient()
	if err != nil {
		return err
	}

	auth := authn.DefaultKeychain
	reader, err := pullBundleSource(logger, client, image, auth)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err := unpackBundleTarToDirectory(logger, reader, manifestDir, outputDir); err != nil {
		return err
	}

	return nil
}

func pullBundleSource(
	logger logrus.FieldLogger,
	client kubernetes.Interface,
	bundleSource string,
	auth authn.Keychain,
) (io.ReadCloser, error) {
	ref, err := name.ParseReference(bundleSource)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	logger.Infof("attempting to pull the %s bundle image", bundleSource)
	image, err := remote.Image(ref, remote.WithAuthFromKeychain(authn.NewMultiKeychain(auth)))
	if err != nil {
		return nil, err
	}
	size, err := image.Size()
	if err != nil {
		return nil, err
	}
	logger.Infof("unpacked image has size %v", size)

	return mutate.Extract(image), nil
}

func unpackBundleTarToDirectory(
	logger logrus.FieldLogger,
	reader io.ReadCloser,
	manifestDir,
	outputDir string,
) error {
	logger.Infof("unpacking filesystem content from the %s directory to the %s output directory", manifestDir, outputDir)
	t := tar.NewReader(reader)

	for true {
		header, err := t.Next()
		if err == io.EOF {
			logger.Debug("processed the EOF marker")
			break
		}
		if err != nil {
			return fmt.Errorf("failed to unpack bundle image: %v", err)
		}

		// filter out any non-manifest filesystem directories
		// TODO(tflannag): Is it reasonable to always assume that
		// we're going to be handling scratch images that contain
		// a series of static files? If that's the case, then we
		// largely only care about _where_ to store this unpacked
		// files.
		target := filepath.Clean(header.Name)
		if !strings.Contains(target, manifestDir) {
			continue
		}
		logger.Infof("processing the manifest file %s", target)

		switch header.Typeflag {
		case tar.TypeDir:
			logger.Infof("attempting to mkdir the %s output directory if it doesn't already exist", outputDir)
			os.Mkdir(outputDir, 0755)

		case tar.TypeReg:
			outFile, err := os.Create(filepath.Join(outputDir, filepath.Base(target)))
			if err != nil {
				return err
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, t); err != nil {
				return err
			}
		}
	}
	return nil
}

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
