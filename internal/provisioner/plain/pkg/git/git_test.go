package git

import (
	"fmt"
	"testing"

	"github.com/operator-framework/rukpak/api/v1alpha1"
)

func TestCheckoutCommand(t *testing.T) {
	var gitSources = []struct {
		source   v1alpha1.GitSource
		expected string
	}{
		{
			source: v1alpha1.GitSource{
				Repository: "https://github.com/operator-framework/combo",
				Ref: v1alpha1.GitRef{
					Commit: "4567031e158b42263e70a7c63e29f8981a4a6135",
				},
			},
			expected: fmt.Sprintf("git clone %s && cd %s && git checkout %s && cp -r %s/* /manifests",
				"https://github.com/operator-framework/combo", "combo", "4567031e158b42263e70a7c63e29f8981a4a6135",
				"./manifests"),
		},
		{
			source: v1alpha1.GitSource{
				Repository: "https://github.com/operator-framework/combo",
				Ref: v1alpha1.GitRef{
					Tag: "v0.0.1",
				},
			},
			expected: fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout tags/%s && cp -r %s/* /manifests",
				"v0.0.1", "https://github.com/operator-framework/combo", "combo", "v0.0.1", "./manifests"),
		},
		{
			source: v1alpha1.GitSource{
				Repository: "https://github.com/operator-framework/combo",
				Directory:  "./deploy",
				Ref: v1alpha1.GitRef{
					Branch: "dev",
				},
			},
			expected: fmt.Sprintf("git clone --depth 1 --branch %s %s && cd %s && git checkout %s && cp -r %s/* /manifests",
				"dev", "https://github.com/operator-framework/combo", "combo", "dev", "./deploy"),
		},
	}

	for _, tt := range gitSources {
		result := CheckoutCommand(tt.source)
		if result != tt.expected {
			t.Fatalf("expected %s, got %s", tt.expected, result)
		}
	}
}
