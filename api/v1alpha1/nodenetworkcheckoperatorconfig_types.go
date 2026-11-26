/*
Copyright 2026.

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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NetworkDetectionMode selects how node network health is checked.
type NetworkDetectionMode string

const (
	// NetworkDetectionModeService uses an existing service on each node.
	NetworkDetectionModeService NetworkDetectionMode = "Service"
	// NetworkDetectionModeTCPListener checks a configured TCP endpoint from each node.
	NetworkDetectionModeTCPListener NetworkDetectionMode = "TCPListener"
	// NetworkDetectionModeBoth uses both detection methods.
	NetworkDetectionModeBoth NetworkDetectionMode = "Both"
)

// NodeConditionServiceUnhealthy is set when the node's kubelet service cannot
// be reached after the configured number of consecutive checks.
const NodeConditionServiceUnhealthy corev1.NodeConditionType = "NNCOServiceUnhealthy"

// NodeConditionTCPListenerUnhealthy reports consecutive failures to reach the configured TCP endpoint.
const NodeConditionTCPListenerUnhealthy corev1.NodeConditionType = "NNCOTCPListenerUnhealthy"

type NetworkCheckType string

const NetworkCheckTypeTCPSocket NetworkCheckType = "tcpSocket"

// NetworkCheck specifies an endpoint to probe from each node.
type NetworkCheck struct {
	// Type selects the check mechanism.
	// +kubebuilder:validation:Enum=tcpSocket
	Type NetworkCheckType `json:"type"`

	// Host is an IPv4 or IPv6 address or a DNS name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Host string `json:"host"`

	// Port is the TCP port on the destination host.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// NodeNetworkCheckOperatorConfigSpec defines the desired state of NodeNetworkCheckOperatorConfig
// +kubebuilder:validation:XValidation:rule="!has(self.networkDetectionMode) || self.networkDetectionMode == 'Service' || (has(self.checks) && size(self.checks) == 1)",message="TCPListener and Both require one TCP endpoint in checks"
type NodeNetworkCheckOperatorConfigSpec struct {
	// NetworkDetectionMode selects Service, TCPListener, or Both. Service uses an
	// existing service such as the kubelet. TCPListener checks a configured TCP endpoint from each node.
	// Defaults to Service.
	// +kubebuilder:validation:Enum=Service;TCPListener;Both
	// +kubebuilder:default=Service
	// +optional
	NetworkDetectionMode *NetworkDetectionMode `json:"networkDetectionMode,omitempty"`

	// Checks specifies the outbound TCP check. TCPListener and Both require one entry.
	// +kubebuilder:validation:MaxItems=1
	// +optional
	Checks []NetworkCheck `json:"checks,omitempty"`

	// NetworkCheckIntervalSeconds is the time in seconds between network checks.
	// Defaults to 5 seconds.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	// +optional
	NetworkCheckIntervalSeconds *int32 `json:"networkCheckIntervalSeconds,omitempty"`

	// ConsecutiveFailuresThreshold is the number of consecutive failed network
	// checks after which a node is considered unhealthy. Defaults to 3.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	ConsecutiveFailuresThreshold *int32 `json:"consecutiveFailuresThreshold,omitempty"`
}

// NodeNetworkCheckOperatorConfigStatus defines the observed state of NodeNetworkCheckOperatorConfig
type NodeNetworkCheckOperatorConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// NodeNetworkCheckOperatorConfig is the Schema for the nodenetworkcheckoperatorconfigs API
type NodeNetworkCheckOperatorConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:default={}
	Spec   NodeNetworkCheckOperatorConfigSpec   `json:"spec,omitempty"`
	Status NodeNetworkCheckOperatorConfigStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// NodeNetworkCheckOperatorConfigList contains a list of NodeNetworkCheckOperatorConfig
type NodeNetworkCheckOperatorConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeNetworkCheckOperatorConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeNetworkCheckOperatorConfig{}, &NodeNetworkCheckOperatorConfigList{})
}
