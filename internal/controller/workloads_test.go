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
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

var testTCPCheck = networkv1alpha1.NetworkCheck{
	Type: networkv1alpha1.NetworkCheckTypeTCPSocket, Host: "endpoint.example.com", Port: 443,
}

var _ = Describe("Network check workloads", func() {
	It("replaces check workloads on config changes and removes legacy monitors", func() {
		ctx := context.Background()
		const namespace = "default"
		config := &networkv1alpha1.NodeNetworkCheckOperatorConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "workload-test", Namespace: namespace},
		}
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, config)).To(Succeed()) })
		key := client.ObjectKeyFromObject(config)
		r := &NodeNetworkCheckOperatorConfigReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Workloads: &WorkloadSettings{
				Namespace: namespace, Image: "example.invalid/nnco:test",
				TCPMonitorServiceAccount: "tcp-agent", MonitorServiceAccount: "manager",
			},
		}
		reconcile := func() ctrl.Result {
			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			return result
		}
		changeConfig := func(mode networkv1alpha1.NetworkDetectionMode, port int32) {
			Expect(k8sClient.Get(ctx, key, config)).To(Succeed())
			config.Spec.NetworkDetectionMode = &mode
			config.Spec.Checks = nil
			if mode != networkv1alpha1.NetworkDetectionModeService {
				check := testTCPCheck
				check.Port = port
				config.Spec.Checks = []networkv1alpha1.NetworkCheck{check}
			}
			Expect(k8sClient.Update(ctx, config)).To(Succeed())
		}
		service := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: string(ServiceMonitor), Namespace: namespace}}
		tcpMonitor := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: string(TCPMonitor), Namespace: namespace}}
		legacyHandshake := r.serviceMonitorDeployment(config)
		legacyHandshake.Name = "handshake-monitor"
		legacyListener := r.tcpMonitorDaemonSet(config)
		legacyListener.Name = "tcp-listener"
		for _, obj := range []client.Object{legacyHandshake, legacyListener} {
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		}
		for _, obj := range []client.Object{service, tcpMonitor, legacyHandshake, legacyListener} {
			DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed()) })
		}
		get := func(obj client.Object) error {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		}
		// Envtest has no garbage collector to complete foreground deletion.
		completeDeletion := func(obj client.Object) {
			Expect(get(obj)).To(Succeed())
			Expect(obj.GetDeletionTimestamp()).NotTo(BeNil())
			obj.SetFinalizers(nil)
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
			Expect(apierrors.IsNotFound(get(obj))).To(BeTrue())
		}

		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		completeDeletion(legacyHandshake)
		completeDeletion(legacyListener)
		reconcile()
		Expect(get(service)).To(Succeed())
		Expect(apierrors.IsNotFound(get(tcpMonitor))).To(BeTrue())

		changeConfig(networkv1alpha1.NetworkDetectionModeTCPListener, 443)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		Expect(apierrors.IsNotFound(get(tcpMonitor))).To(BeTrue())
		completeDeletion(service)
		reconcile()
		Expect(get(tcpMonitor)).To(Succeed())
		Expect(apierrors.IsNotFound(get(service))).To(BeTrue())
		pod := tcpMonitor.Spec.Template.Spec
		Expect(pod.HostNetwork).To(BeTrue())
		Expect(pod.ServiceAccountName).To(Equal("tcp-agent"))
		Expect(pod.Containers[0].Args).To(ContainElement("--agent=tcp-monitor"))
		Expect(pod.Containers[0].Env[0].ValueFrom.FieldRef.FieldPath).To(Equal("spec.nodeName"))
		Expect(pod.Containers[0].Ports).To(BeEmpty())
		previousTCPMonitor := tcpMonitor.UID

		changeConfig(networkv1alpha1.NetworkDetectionModeBoth, 8443)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		completeDeletion(tcpMonitor)
		reconcile()
		Expect(get(tcpMonitor)).To(Succeed())
		Expect(tcpMonitor.UID).NotTo(Equal(previousTCPMonitor))
		Expect(tcpMonitor.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--config-version=" + workloadVersion(config)))
		Expect(get(service)).To(Succeed())
		previousService := service.UID

		changeConfig(networkv1alpha1.NetworkDetectionModeService, 0)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		completeDeletion(tcpMonitor)
		completeDeletion(service)
		reconcile()
		Expect(get(service)).To(Succeed())
		Expect(service.UID).NotTo(Equal(previousService))
		Expect(apierrors.IsNotFound(get(tcpMonitor))).To(BeTrue())
	})

	It("marks only its node after consecutive TCP failures and clears the condition on recovery", func() {
		ctx := context.Background()
		mode := networkv1alpha1.NetworkDetectionModeBoth
		threshold := int32(2)
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		addr := listener.Addr().(*net.TCPAddr)
		Expect(listener.Close()).To(Succeed())
		config := &networkv1alpha1.NodeNetworkCheckOperatorConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "tcp-monitor-test", Namespace: "default"},
			Spec: networkv1alpha1.NodeNetworkCheckOperatorConfigSpec{
				NetworkDetectionMode: &mode, ConsecutiveFailuresThreshold: &threshold,
				Checks: []networkv1alpha1.NetworkCheck{{
					Type: networkv1alpha1.NetworkCheckTypeTCPSocket, Host: addr.IP.String(), Port: int32(addr.Port),
				}},
			},
		}
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, config)).To(Succeed()) })
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "tcp-monitor-test-node"}}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })
		node.Status.Conditions = []corev1.NodeCondition{{Type: ServiceMonitor.condition(), Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		otherNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other-tcp-monitor-test-node"}}
		Expect(k8sClient.Create(ctx, otherNode)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, otherNode)).To(Succeed()) })

		r := &NodeNetworkCheckOperatorConfigReconciler{Client: k8sClient}
		settings, err := settingsFor(config)
		Expect(err).NotTo(HaveOccurred())
		var failures int32
		check := func() *corev1.NodeCondition {
			Expect(r.checkLocalTCP(ctx, config, settings, node.Name, &failures)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
			return nodeCondition(node, TCPMonitor.condition())
		}
		Expect(check()).To(BeNil())
		condition := check()
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(corev1.ConditionTrue))
		Expect(condition.Reason).To(Equal("TCPEndpointUnreachable"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(otherNode), otherNode)).To(Succeed())
		Expect(nodeCondition(otherNode, TCPMonitor.condition())).To(BeNil())

		listener, err = net.ListenTCP("tcp4", addr)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(listener.Close)
		Expect(check()).To(BeNil())
		Expect(failures).To(BeZero())
		Expect(serviceConditionTrue(node)).To(BeTrue())
	})
})
