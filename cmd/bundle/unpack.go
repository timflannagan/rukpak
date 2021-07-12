package main

import (
	"github.com/spf13/cobra"
)

func newUnpackCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unpack",
		Short: "Unpack arbitrary bundle content from a container image",
		RunE:  run,
	}

	// TODO(tflannag): Somehow inheriting this `--azure-container-registry-config` flag.
	cmd.Flags().String("image", "", "configures the container image that contains bundle content")
	cmd.Flags().String("namespace", "rukpak", "configures the namespace for in-cluster authentication")
	cmd.Flags().String("service-account-name", "rukpak", "configures the serviceaccount metadata.Name for in-cluster authentication")
	cmd.Flags().String("manifest-dir", "manifests", "configures the unpacker to unpack contents stored in the container image filesystem")
	cmd.Flags().String("output-dir", "manifests", "configures the directory to store unpacked bundle content")

	return cmd
}
