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
	"net"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

var _ = Describe("Kubelet service checks", func() {
	const (
		configName = "service-check-config"
		nodeA      = "service-check-node-a"
		nodeB      = "service-check-node-b"
		addressA   = "192.0.2.10"
		addressB   = "192.0.2.11"
	)

	var (
		ctx        context.Context
		clock      time.Time
		config     *networkv1alpha1.NodeNetworkCheckOperatorConfig
		reconciler *NodeNetworkCheckOperatorConfigReconciler
		available  map[string]bool
		calls      map[string]int
		probeMutex sync.Mutex
	)

	createNode := func(name, address string) {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		node.Status.Conditions = []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "Ready"},
			{Type: corev1.NodeConditionType("OtherUnhealthy"), Status: corev1.ConditionTrue, Reason: "Other"},
		}
		if address != "" {
			node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}
		}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
	}

	getNode := func(name string) *corev1.Node {
		node := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
		return node
	}

	reconcile := func() ctrl.Result {
		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(config)})
		Expect(err).NotTo(HaveOccurred())
		return result
	}

	BeforeEach(func() {
		ctx = context.Background()
		clock = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		config = &networkv1alpha1.NodeNetworkCheckOperatorConfig{
			ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: "default"},
		}
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		available = map[string]bool{addressA: false, addressB: true}
		calls = make(map[string]int)
		reconciler = &NodeNetworkCheckOperatorConfigReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			now:       func() time.Time { return clock },
			probeService: func(_ context.Context, address string) error {
				probeMutex.Lock()
				defer probeMutex.Unlock()
				calls[address]++
				if available[address] {
					return nil
				}
				return errors.New("connection failed")
			},
		}
	})

	AfterEach(func() {
		for _, name := range []string{nodeA, nodeB} {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}))).To(Succeed())
		}
		for _, name := range []string{configName, "second-service-config"} {
			resource := &networkv1alpha1.NodeNetworkCheckOperatorConfig{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, resource))).To(Succeed())
		}
	})

	It("marks a node after consecutive failures and removes only its condition on recovery", func() {
		createNode(nodeA, addressA)
		createNode(nodeB, addressB)

		Expect(reconcile().RequeueAfter).To(Equal(5 * time.Second))
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		reconcile() // A config event must not count another check before the interval.
		Expect(calls[addressA]).To(Equal(1))

		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(5 * time.Second)
		reconcile()
		unhealthy := serviceCondition(getNode(nodeA))
		Expect(unhealthy).NotTo(BeNil())
		Expect(unhealthy.Status).To(Equal(corev1.ConditionTrue))
		Expect(unhealthy.Reason).To(Equal(serviceFailureReason))
		Expect(serviceCondition(getNode(nodeB))).To(BeNil())

		transition := unhealthy.LastTransitionTime
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceCondition(getNode(nodeA)).LastTransitionTime).To(Equal(transition))

		available[addressA] = true
		clock = clock.Add(5 * time.Second)
		reconcile()
		recovered := getNode(nodeA)
		Expect(serviceCondition(recovered)).To(BeNil())
		Expect(recovered.Status.Conditions).To(ConsistOf(
			corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "Ready"},
			corev1.NodeCondition{Type: corev1.NodeConditionType("OtherUnhealthy"), Status: corev1.ConditionTrue, Reason: "Other"},
		))
	})

	It("resets the failure count after a successful check or node address change", func() {
		createNode(nodeA, addressA)
		reconcile()
		clock = clock.Add(5 * time.Second)
		reconcile()
		available[addressA] = true
		clock = clock.Add(5 * time.Second)
		reconcile()
		available[addressA] = false
		clock = clock.Add(5 * time.Second)
		reconcile()
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())

		node := getNode(nodeA)
		node.Status.Addresses[0].Address = addressB
		available[addressB] = false
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		reconcile() // A new address starts a fresh failure streak.
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceConditionTrue(getNode(nodeA))).To(BeTrue())
	})

	It("does not mark a node unhealthy when no usable address is available", func() {
		createNode(nodeA, "")
		for i := 0; i < 4; i++ {
			reconcile()
			clock = clock.Add(5 * time.Second)
		}
		Expect(calls).To(BeEmpty())
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
	})

	It("does not apply a result for an address that changed during the probe", func() {
		createNode(nodeA, addressA)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		threshold := int32(1)
		config.Spec.ConsecutiveFailuresThreshold = &threshold
		Expect(k8sClient.Update(ctx, config)).To(Succeed())
		reconciler.probeService = func(ctx context.Context, address string) error {
			if address == addressA {
				node := &corev1.Node{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: nodeA}, node); err != nil {
					return err
				}
				node.Status.Addresses[0].Address = addressB
				if err := k8sClient.Status().Update(ctx, node); err != nil {
					return err
				}
			}
			return errors.New("connection failed")
		}
		reconcile()
		node := getNode(nodeA)
		Expect(node.Status.Addresses[0].Address).To(Equal(addressB))
		Expect(serviceCondition(node)).To(BeNil())
		reconcile()
		Expect(serviceConditionTrue(getNode(nodeA))).To(BeTrue())
	})

	It("does not apply a result after the configuration changes during the probe", func() {
		createNode(nodeA, addressA)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		threshold := int32(1)
		config.Spec.ConsecutiveFailuresThreshold = &threshold
		Expect(k8sClient.Update(ctx, config)).To(Succeed())
		reconciler.probeService = func(ctx context.Context, _ string) error {
			current := &networkv1alpha1.NodeNetworkCheckOperatorConfig{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(config), current); err != nil {
				return err
			}
			mode := networkv1alpha1.NetworkDetectionModeTCPListener
			current.Spec.NetworkDetectionMode = &mode
			if err := k8sClient.Update(ctx, current); err != nil {
				return err
			}
			return errors.New("connection failed")
		}
		Expect(reconcile().Requeue).To(BeTrue())
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
	})

	It("respects the interval when settings change during a probe", func() {
		createNode(nodeA, addressA)
		attempts := 0
		reconciler.probeService = func(ctx context.Context, _ string) error {
			attempts++
			if attempts == 1 {
				current := &networkv1alpha1.NodeNetworkCheckOperatorConfig{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(config), current); err != nil {
					return err
				}
				threshold := int32(2)
				current.Spec.ConsecutiveFailuresThreshold = &threshold
				if err := k8sClient.Update(ctx, current); err != nil {
					return err
				}
			}
			return errors.New("connection failed")
		}
		Expect(reconcile().Requeue).To(BeTrue())
		Expect(reconcile().RequeueAfter).To(Equal(5 * time.Second))
		Expect(attempts).To(Equal(1))
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(5 * time.Second)
		reconcile()
		Expect(serviceConditionTrue(getNode(nodeA))).To(BeTrue())
	})

	It("uses configured thresholds and stops the service check in TCPListener mode", func() {
		createNode(nodeA, addressA)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		interval, threshold := int32(1), int32(2)
		mode := networkv1alpha1.NetworkDetectionModeBoth
		config.Spec.NetworkCheckIntervalSeconds = &interval
		config.Spec.ConsecutiveFailuresThreshold = &threshold
		config.Spec.NetworkDetectionMode = &mode
		Expect(k8sClient.Update(ctx, config)).To(Succeed())
		Expect(reconcile().RequeueAfter).To(Equal(time.Second))
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(time.Second)
		reconcile()
		Expect(serviceConditionTrue(getNode(nodeA))).To(BeTrue())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		mode = networkv1alpha1.NetworkDetectionModeTCPListener
		config.Spec.NetworkDetectionMode = &mode
		Expect(k8sClient.Update(ctx, config)).To(Succeed())
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		clock = clock.Add(time.Minute)
		reconcile()
		Expect(calls[addressA]).To(Equal(2))
	})

	It("clears its condition when the last configuration is deleted", func() {
		createNode(nodeA, addressA)
		for i := 0; i < 3; i++ {
			reconcile()
			clock = clock.Add(5 * time.Second)
		}
		Expect(serviceConditionTrue(getNode(nodeA))).To(BeTrue())
		Expect(k8sClient.Delete(ctx, config)).To(Succeed())
		reconcile()
		Expect(serviceCondition(getNode(nodeA))).To(BeNil())
		Expect(getNode(nodeA).Status.Conditions).To(HaveLen(2))
	})

	It("selects one configuration and lets the next take over", func() {
		createNode(nodeA, addressA)
		second := &networkv1alpha1.NodeNetworkCheckOperatorConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "second-service-config", Namespace: "default"},
		}
		Expect(k8sClient.Create(ctx, second)).To(Succeed())
		active, count, err := activeConfiguration(ctx, k8sClient)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(2))
		inactive := second
		if active.Name == second.Name {
			inactive = config
		}
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: inactive.Name, Namespace: inactive.Namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(BeEmpty())
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(active)})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls[addressA]).To(Equal(1))
		Expect(k8sClient.Delete(ctx, active)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(inactive)})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls[addressA]).To(Equal(2))
	})

	It("retries a node status conflict without losing another condition", func() {
		createNode(nodeA, addressA)
		wrapped := &conflictOnceClient{Client: k8sClient}
		reconciler.Client = wrapped
		Expect(reconciler.updateServiceCondition(ctx, getNode(nodeA), true, 3, config, true)).To(Succeed())
		updated := getNode(nodeA)
		Expect(serviceConditionTrue(updated)).To(BeTrue())
		Expect(updated.Status.Conditions).To(ContainElement(corev1.NodeCondition{
			Type: corev1.NodeConditionType("ConcurrentUpdate"), Status: corev1.ConditionTrue,
		}))
	})

	It("runs periodic checks through the manager and reacts to a mode change", func() {
		createNode(nodeA, addressA)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		interval, threshold := int32(1), int32(2)
		config.Spec.NetworkCheckIntervalSeconds = &interval
		config.Spec.ConsecutiveFailuresThreshold = &threshold
		Expect(k8sClient.Update(ctx, config)).To(Succeed())

		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
		})
		Expect(err).NotTo(HaveOccurred())
		managed := &NodeNetworkCheckOperatorConfigReconciler{
			Client:    manager.GetClient(),
			APIReader: manager.GetAPIReader(),
			Scheme:    manager.GetScheme(),
			probeService: func(context.Context, string) error {
				return errors.New("connection failed")
			},
		}
		Expect(managed.SetupWithManager(manager)).To(Succeed())
		managerCtx, cancel := context.WithCancel(ctx)
		managerDone := make(chan error, 1)
		go func() { managerDone <- manager.Start(managerCtx) }()
		defer func() {
			cancel()
			Expect(<-managerDone).To(Succeed())
		}()

		Eventually(func() bool { return serviceConditionTrue(getNode(nodeA)) }, 20*time.Second, 100*time.Millisecond).Should(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		mode := networkv1alpha1.NetworkDetectionModeTCPListener
		config.Spec.NetworkDetectionMode = &mode
		Expect(k8sClient.Update(ctx, config)).To(Succeed())
		Eventually(func() bool { return serviceCondition(getNode(nodeA)) == nil }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
	})

	It("cleans up a stale condition if the configuration was deleted while stopped", func() {
		createNode(nodeA, addressA)
		node := getNode(nodeA)
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type: networkv1alpha1.NodeConditionServiceUnhealthy, Status: corev1.ConditionTrue,
		})
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		Expect(k8sClient.Delete(ctx, config)).To(Succeed())

		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
		})
		Expect(err).NotTo(HaveOccurred())
		managed := &NodeNetworkCheckOperatorConfigReconciler{
			Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Scheme: manager.GetScheme(),
		}
		Expect(managed.SetupWithManager(manager)).To(Succeed())
		managerCtx, cancel := context.WithCancel(ctx)
		managerDone := make(chan error, 1)
		go func() { managerDone <- manager.Start(managerCtx) }()
		defer func() {
			cancel()
			Expect(<-managerDone).To(Succeed())
		}()
		Eventually(func() bool { return serviceCondition(getNode(nodeA)) == nil }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		Expect(getNode(nodeA).Status.Conditions).To(HaveLen(2))
	})
})

type conflictOnceClient struct {
	client.Client
	conflicted bool
}

func (c *conflictOnceClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type conflictOnceWriter struct {
	client.SubResourceWriter
	client *conflictOnceClient
}

func (w *conflictOnceWriter) Update(ctx context.Context, object client.Object, opts ...client.SubResourceUpdateOption) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		current := &corev1.Node{}
		if err := w.client.Client.Get(ctx, client.ObjectKeyFromObject(object), current); err != nil {
			return err
		}
		current.Status.Conditions = append(current.Status.Conditions, corev1.NodeCondition{
			Type: corev1.NodeConditionType("ConcurrentUpdate"), Status: corev1.ConditionTrue,
		})
		if err := w.client.Client.Status().Update(ctx, current); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, object.GetName(), errors.New("concurrent status update"))
	}
	return w.SubResourceWriter.Update(ctx, object, opts...)
}

var _ = Describe("Kubelet TCP probe", func() {
	It("tries each internal IP and succeeds if one accepts connections", func() {
		node := &corev1.Node{Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeExternalIP, Address: "192.0.2.1"},
			{Type: corev1.NodeInternalIP, Address: "192.0.2.1"},
			{Type: corev1.NodeInternalIP, Address: "2001:db8::1"},
		}}}
		var attempted []string
		r := &NodeNetworkCheckOperatorConfigReconciler{probeService: func(_ context.Context, address string) error {
			attempted = append(attempted, address)
			if address == "2001:db8::1" {
				return nil
			}
			return errors.New("unreachable")
		}}
		Expect(r.checkKubelet(context.Background(), node)).To(Succeed())
		Expect(attempted).To(Equal([]string{"192.0.2.1", "2001:db8::1"}))
	})

	It("opens a TCP connection to the kubelet port", func() {
		listener, err := net.Listen("tcp", "127.0.0.1:"+kubeletPort)
		if err != nil {
			Skip("kubelet port is already in use")
		}
		defer listener.Close()
		Expect(probeKubeletService(context.Background(), "127.0.0.1")).To(Succeed())
	})
})
