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

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

const (
	workloadLabel      = "app.kubernetes.io/name"
	managedByLabel     = "app.kubernetes.io/managed-by"
	managedBy          = "node-network-check-operator"
	configVersionKey   = "node-network-check-operator.medik8s.io/config-version"
	workloadRetryDelay = time.Second
)

// WorkloadSettings identifies the operator's namespace, image and service accounts.
type WorkloadSettings struct {
	Namespace                string
	Image                    string
	MonitorServiceAccount    string
	TCPMonitorServiceAccount string
}

func workloadLabels(name string) map[string]string {
	return map[string]string{workloadLabel: name, managedByLabel: managedBy}
}

func workloadVersion(config *networkv1alpha1.NodeNetworkCheckOperatorConfig) string {
	return fmt.Sprintf("%s:%d", config.UID, config.Generation)
}

func (r *NodeNetworkCheckOperatorConfigReconciler) reconcileWorkloads(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, settings networkCheckSettings) (ctrl.Result, error) {
	desired := []client.Object{}
	version := configVersion{uid: config.UID, generation: config.Generation}
	if TCPMonitor.enabled(settings.mode) {
		if settings.tcpCheck != nil {
			desired = append(desired, r.tcpMonitorDaemonSet(config))
		} else if r.activeConfig != version {
			// Configurations stored before the CRD upgrade may lack an endpoint.
			log.FromContext(ctx).Info("TCP monitor requires one TCP endpoint in checks",
				"config", client.ObjectKeyFromObject(config))
		}
	}
	if ServiceMonitor.enabled(settings.mode) {
		desired = append(desired, r.serviceMonitorDeployment(config))
	}
	for _, obj := range desired {
		annotations := map[string]string{configVersionKey: workloadVersion(config)}
		obj.SetAnnotations(annotations)
		workloadTemplate(obj).Annotations = annotations
		if config.Namespace == obj.GetNamespace() {
			if err := controllerutil.SetControllerReference(config, obj, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	result, err := r.cleanNetworkChecks(ctx, config, desired)
	if err != nil || !result.IsZero() {
		return result, err
	}

	r.activeConfig = version
	// Recheck the workloads even in Service mode so a deleted workload is recreated.
	return ctrl.Result{RequeueAfter: defaultCheckInterval}, nil
}

func (r *NodeNetworkCheckOperatorConfigReconciler) cleanNetworkChecks(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, desired []client.Object) (ctrl.Result, error) {
	enabled := map[string]bool{}
	for _, obj := range desired {
		enabled[obj.GetName()] = true
	}
	missing := desired
	if r.Workloads != nil {
		var wait bool
		var err error
		missing, wait, err = r.retireWorkloads(ctx, config, desired)
		if err != nil {
			return ctrl.Result{}, err
		}
		if wait {
			return ctrl.Result{RequeueAfter: workloadRetryDelay}, nil
		}
	}
	if config == nil || r.activeConfig != (configVersion{uid: config.UID, generation: config.Generation}) {
		for _, monitor := range []MonitorKind{ServiceMonitor, TCPMonitor} {
			if !enabled[string(monitor)] {
				result, err := r.clearMonitorConditions(ctx, config, monitor)
				if err != nil || !result.IsZero() {
					return result, err
				}
			}
		}
	}
	if len(missing) > 0 {
		matches, err := r.configurationMatches(ctx, config)
		if err != nil || !matches {
			return ctrl.Result{Requeue: !matches}, err
		}
	}
	for _, obj := range missing {
		if err := r.Create(ctx, obj); err != nil {
			return ctrl.Result{}, fmt.Errorf("create %s: %w", client.ObjectKeyFromObject(obj), err)
		}
	}
	return ctrl.Result{}, nil
}

func (r *NodeNetworkCheckOperatorConfigReconciler) retireWorkloads(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, desired []client.Object) ([]client.Object, bool, error) {
	namespace := r.Workloads.Namespace
	wanted := make(map[string]client.Object, len(desired))
	for _, obj := range desired {
		wanted[obj.GetName()] = obj
	}
	wait := false
	for _, obj := range []client.Object{
		// Remove workloads left by installations that used inbound TCP checks.
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "tcp-listener", Namespace: namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "handshake-monitor", Namespace: namespace}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: string(TCPMonitor), Namespace: namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: string(ServiceMonitor), Namespace: namespace}},
	} {
		key := client.ObjectKeyFromObject(obj)
		if err := r.reader().Get(ctx, key, obj); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return nil, false, err
		}

		want := wanted[key.Name]
		if obj.GetLabels()[managedByLabel] != managedBy {
			if want != nil {
				return nil, false, fmt.Errorf("workload %s is not managed by this operator", key)
			}
			continue
		}
		if obj.GetDeletionTimestamp() != nil {
			wait = true
			continue
		}
		containers := workloadTemplate(obj).Spec.Containers
		if want != nil && obj.GetAnnotations()[configVersionKey] == workloadVersion(config) &&
			len(containers) > 0 && containers[0].Image == r.Workloads.Image {
			delete(wanted, key.Name)
			continue
		}
		matches, err := r.configurationMatches(ctx, config)
		if err != nil {
			return nil, false, err
		}
		if !matches {
			return nil, true, nil
		}
		uid := obj.GetUID()
		if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationForeground), client.Preconditions{UID: &uid}); client.IgnoreNotFound(err) != nil {
			return nil, false, fmt.Errorf("delete %s: %w", key, err)
		}
		wait = true
	}
	var missing []client.Object
	for _, obj := range desired {
		if wanted[obj.GetName()] != nil {
			missing = append(missing, obj)
		}
	}
	return missing, wait, nil
}

func workloadTemplate(obj client.Object) *corev1.PodTemplateSpec {
	switch typed := obj.(type) {
	case *appsv1.DaemonSet:
		return &typed.Spec.Template
	case *appsv1.Deployment:
		return &typed.Spec.Template
	}
	panic("unsupported workload kind")
}

func (r *NodeNetworkCheckOperatorConfigReconciler) tcpMonitorDaemonSet(config *networkv1alpha1.NodeNetworkCheckOperatorConfig) *appsv1.DaemonSet {
	name := string(TCPMonitor)
	template := r.monitorPod(TCPMonitor, config)
	template.Spec.HostNetwork = true
	template.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	template.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	template.Spec.ServiceAccountName = r.Workloads.TCPMonitorServiceAccount
	template.Spec.Containers[0].Env = []corev1.EnvVar{{
		Name:      "NNCO_NODE_NAME",
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}},
	}}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Workloads.Namespace, Labels: workloadLabels(name)},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: workloadLabels(name)},
			Template: template,
		},
	}
}

func (r *NodeNetworkCheckOperatorConfigReconciler) serviceMonitorDeployment(config *networkv1alpha1.NodeNetworkCheckOperatorConfig) *appsv1.Deployment {
	name := string(ServiceMonitor)
	template := r.monitorPod(ServiceMonitor, config)
	template.Spec.ServiceAccountName = r.Workloads.MonitorServiceAccount
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Workloads.Namespace, Labels: workloadLabels(name)},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: workloadLabels(name)},
			Template: template,
		},
	}
}

func (r *NodeNetworkCheckOperatorConfigReconciler) monitorPod(monitor MonitorKind, config *networkv1alpha1.NodeNetworkCheckOperatorConfig) corev1.PodTemplateSpec {
	name := string(monitor)
	return r.workloadPod(name, []string{
		"--agent=" + name,
		"--config-name=" + config.Name,
		"--config-namespace=" + config.Namespace,
		"--config-version=" + workloadVersion(config),
	})
}

func (r *NodeNetworkCheckOperatorConfigReconciler) workloadPod(name string, args []string) corev1.PodTemplateSpec {
	yes, no := true, false
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: workloadLabels(name)},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &yes,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name: name, Image: r.Workloads.Image, Command: []string{"/manager"}, Args: args,
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &no,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
			}},
		},
	}
}
