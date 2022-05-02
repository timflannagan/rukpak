package util

import (
	"reflect"
	"testing"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
)

func TestCheckExistingBundlesMatchesTemplate(t *testing.T) {
	type args struct {
		existingBundles       []*rukpakv1alpha1.Bundle
		desiredBundleTemplate *rukpakv1alpha1.BundleTemplate
	}
	tests := []struct {
		name string
		args args
		want *rukpakv1alpha1.Bundle
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckExistingBundlesMatchesTemplate(tt.args.existingBundles, tt.args.desiredBundleTemplate); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CheckExistingBundlesMatchesTemplate() = %v, want %v", got, tt.want)
			}
		})
	}
}
