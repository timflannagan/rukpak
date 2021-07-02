/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type BundleSourceType string

type BundleVolumeMountConfigMap struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// BundleVolumeMount is the specification of a single
// volume that contains bundle manifest content and
// how to mount that content.
type BundleVolumeMount struct {
	// MountPath is the filesystem path to the bundle
	// manifest content
	MountPath string `json:"mountPath"`
	// ConfigMap is a type of BundleVolumeMount that contains
	// the requisite metadata needed to query for an existing
	// ConfigMap with the provided name and namespace fields.
	// +optional
	ConfigMap BundleVolumeMountConfigMap `json:"configMap,omitempty"`
}

// +union
type BundleSource struct {
	// +unionDiscriminator
	// +optional
	VolumeMounts []BundleVolumeMount `json:"volumeMounts,omitempty"`

	// Ref is reference to either local or remote bundle content
	// In the context of local filesystem content, mounted by
	// either a ConfigMap or PV containing Operator manifests(s)
	// then we'd want to prefix that with `file://*` vs. something like
	// `docker://*` in the later case.
	// We may want validation in place such that only image shas can be
	// used. This is helpful in the case where we need to determine whether
	// there's newer manifest content avaiable that should be reconciled
	// and updated, or new manifests are avaiable and we need to install
	// those objects and report back that status by to the BundleStatus field.
	// +optional
	Ref string `json:"ref,omitempty"`
}

// BundleSpec defines the desired state of Bundle
type BundleSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Likely in the future, we'll have the immediate need to handle something
	// like the `olm.bundle` class which has embedded knowledge that understands
	// how to unpack registry+v1 bundle image(s).
	// Class specifies the name of the ProvisionerClass to use for unpacking the bundle.
	Class ProvisionerID `json:"class,omitempty"`

	Source BundleSource `json:"source"`
}

// BundleStatus defines the observed state of Bundle
type BundleStatus struct {
	Conditions []metav1.Condition           `json:"conditions,omitempty"`
	Unpacked   BundleUnpackStatusType       `json:"unpacked"`
	Digest     string                       `json:"digest,omitempty"`
	Volume     *corev1.LocalObjectReference `json:"volume"`
}

type BundleUnpackStatusType string

const (
	BundleUnpacked       BundleUnpackStatusType = "Unpacked"
	BundleNeedsUnpacking BundleUnpackStatusType = "NeedsUnpacking"
)

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:resource:categories=rukpak,scope=Cluster
// +kubebuilder:subresource:status

// Bundle is the Schema for the bundles API
type Bundle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BundleSpec   `json:"spec,omitempty"`
	Status BundleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BundleList contains a list of Bundle
type BundleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bundle `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Bundle{}, &BundleList{})
}
