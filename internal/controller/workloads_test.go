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
	"errors"
	"time"

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

var _ = Describe("Network check workloads", func() {
	It("replaces the workloads on config changes and tears down the listeners in Service mode", func() {
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
				ListenerServiceAccount: "listener", MonitorServiceAccount: "manager",
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
			config.Spec.TCPListenerPort = &port
			Expect(k8sClient.Update(ctx, config)).To(Succeed())
		}
		service := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: string(ServiceMonitor), Namespace: namespace}}
		handshake := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: string(HandshakeMonitor), Namespace: namespace}}
		listener := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: listenerName, Namespace: namespace}}
		for _, obj := range []client.Object{service, handshake, listener} {
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

		reconcile()
		Expect(get(service)).To(Succeed())
		Expect(apierrors.IsNotFound(get(handshake))).To(BeTrue())
		Expect(apierrors.IsNotFound(get(listener))).To(BeTrue())

		changeConfig(networkv1alpha1.NetworkDetectionModeTCPListener, defaultListenerPort)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		Expect(apierrors.IsNotFound(get(listener))).To(BeTrue())
		completeDeletion(service)
		reconcile()
		Expect(get(listener)).To(Succeed())
		Expect(get(handshake)).To(Succeed())
		Expect(apierrors.IsNotFound(get(service))).To(BeTrue())
		Expect(listener.Spec.Template.Spec.HostNetwork).To(BeTrue())
		Expect(listener.Spec.Template.Spec.Containers[0].Ports[0].HostPort).To(Equal(int32(defaultListenerPort)))

		changeConfig(networkv1alpha1.NetworkDetectionModeBoth, 19101)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		completeDeletion(listener)
		completeDeletion(handshake)
		reconcile()
		Expect(get(listener)).To(Succeed())
		Expect(get(handshake)).To(Succeed())
		Expect(get(service)).To(Succeed())
		Expect(listener.Spec.Template.Spec.Containers[0].Ports[0].HostPort).To(Equal(int32(19101)))
		previousService := service.UID

		changeConfig(networkv1alpha1.NetworkDetectionModeService, 19101)
		Expect(reconcile().RequeueAfter).To(Equal(workloadRetryDelay))
		completeDeletion(listener)
		completeDeletion(handshake)
		completeDeletion(service)
		reconcile()
		Expect(get(service)).To(Succeed())
		Expect(service.UID).NotTo(Equal(previousService))
		Expect(apierrors.IsNotFound(get(handshake))).To(BeTrue())
		Expect(apierrors.IsNotFound(get(listener))).To(BeTrue())
	})

	It("reports listener failures after readiness and preserves the service condition on recovery", func() {
		ctx := context.Background()
		mode := networkv1alpha1.NetworkDetectionModeBoth
		threshold := int32(2)
		config := &networkv1alpha1.NodeNetworkCheckOperatorConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "handshake-test", Namespace: "default"},
			Spec: networkv1alpha1.NodeNetworkCheckOperatorConfigSpec{
				NetworkDetectionMode: &mode, ConsecutiveFailuresThreshold: &threshold,
			},
		}
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, config)).To(Succeed()) })
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "handshake-test-node"}}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })
		node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.1"}}
		node.Status.Conditions = []corev1.NodeCondition{{Type: ServiceMonitor.condition(), Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		now := time.Now()
		reachable := false
		attempts := 0
		r := &NodeNetworkCheckOperatorConfigReconciler{
			Client: k8sClient, monitor: HandshakeMonitor, listenerNamespace: "default",
			serviceChecks: make(map[string]*serviceCheckState),
			now:           func() time.Time { return now },
			probeService: func(context.Context, string) error {
				attempts++
				if !reachable {
					return errors.New("listener unreachable")
				}
				return nil
			},
		}
		settings, err := settingsFor(config)
		Expect(err).NotTo(HaveOccurred())
		check := func() *corev1.NodeCondition {
			_, err := r.checkNodes(ctx, config, settings)
			Expect(err).NotTo(HaveOccurred())
			now = now.Add(settings.interval)
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
			return nodeCondition(node, HandshakeMonitor.condition())
		}
		Expect(check()).To(BeNil())
		Expect(attempts).To(BeZero())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "handshake-test-listener", Namespace: "default",
				Labels: workloadLabels(listenerName), Annotations: map[string]string{configVersionKey: workloadVersion(config)},
			},
			Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "listener", Image: "ignored"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() {
			grace := int64(0)
			Expect(k8sClient.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &grace})).To(Succeed())
		})
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		Expect(check()).To(BeNil())
		condition := check()
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(corev1.ConditionTrue))
		reachable = true
		Expect(check()).To(BeNil())
		Expect(serviceConditionTrue(node)).To(BeTrue())
	})
})
