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
	"bytes"
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nodenetworkcheckoperatorv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

var _ = Describe("NodeNetworkCheckOperatorConfig Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		nodenetworkcheckoperatorconfig := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind NodeNetworkCheckOperatorConfig")
			err := k8sClient.Get(ctx, typeNamespacedName, nodenetworkcheckoperatorconfig)
			if err != nil && errors.IsNotFound(err) {
				resource := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance NodeNetworkCheckOperatorConfig")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &NodeNetworkCheckOperatorConfigReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})

		It("logs the current settings when the configuration changes", func() {
			var output bytes.Buffer
			logCtx := log.IntoContext(ctx, zap.New(zap.WriteTo(&output)))
			controllerReconciler := &NodeNetworkCheckOperatorConfigReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			request := reconcile.Request{NamespacedName: typeNamespacedName}

			config := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, config)).To(Succeed())
			Expect(config.Spec.NetworkDetectionMode).NotTo(BeNil())
			Expect(*config.Spec.NetworkDetectionMode).To(Equal(nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeService))
			Expect(config.Spec.NetworkCheckIntervalSeconds).NotTo(BeNil())
			Expect(*config.Spec.NetworkCheckIntervalSeconds).To(Equal(int32(5)))
			Expect(config.Spec.ConsecutiveFailuresThreshold).NotTo(BeNil())
			Expect(*config.Spec.ConsecutiveFailuresThreshold).To(Equal(int32(3)))
			_, err := controllerReconciler.Reconcile(logCtx, request)
			Expect(err).NotTo(HaveOccurred())
			var entry map[string]interface{}
			Expect(json.NewDecoder(&output).Decode(&entry)).To(Succeed())
			Expect(entry).To(HaveKeyWithValue("networkDetectionMode", "Service"))
			Expect(entry).To(HaveKeyWithValue("networkCheckIntervalSeconds", float64(5)))
			Expect(entry).To(HaveKeyWithValue("consecutiveFailuresThreshold", float64(3)))

			interval, threshold := int32(10), int32(4)
			for _, mode := range []nodenetworkcheckoperatorv1alpha1.NetworkDetectionMode{
				nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeTCPListener,
				nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeBoth,
				nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeService,
			} {
				Expect(k8sClient.Get(ctx, typeNamespacedName, config)).To(Succeed())
				config.Spec.NetworkDetectionMode = &mode
				config.Spec.Checks = []nodenetworkcheckoperatorv1alpha1.NetworkCheck{testTCPCheck}
				config.Spec.NetworkCheckIntervalSeconds = &interval
				config.Spec.ConsecutiveFailuresThreshold = &threshold
				Expect(k8sClient.Update(ctx, config)).To(Succeed())
				_, err = controllerReconciler.Reconcile(logCtx, request)
				Expect(err).NotTo(HaveOccurred())
				entry = nil
				Expect(json.NewDecoder(&output).Decode(&entry)).To(Succeed())
				Expect(entry).To(HaveKeyWithValue("networkDetectionMode", string(mode)))
				Expect(entry).To(HaveKeyWithValue("networkCheckIntervalSeconds", float64(10)))
				Expect(entry).To(HaveKeyWithValue("consecutiveFailuresThreshold", float64(4)))
			}

			invalid := nodenetworkcheckoperatorv1alpha1.NetworkDetectionMode("Invalid")
			config.Spec.NetworkDetectionMode = &invalid
			Expect(errors.IsInvalid(k8sClient.Update(ctx, config))).To(BeTrue())
		})

		It("uses the defaults when spec is omitted", func() {
			resource := &unstructured.Unstructured{}
			resource.SetGroupVersionKind(nodenetworkcheckoperatorv1alpha1.GroupVersion.WithKind("NodeNetworkCheckOperatorConfig"))
			resource.SetName("without-spec")
			resource.SetNamespace("default")
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, resource) }()

			mode, found, err := unstructured.NestedString(resource.Object, "spec", "networkDetectionMode")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(mode).To(Equal("Service"))

			interval, found, err := unstructured.NestedInt64(resource.Object, "spec", "networkCheckIntervalSeconds")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(interval).To(Equal(int64(5)))

			threshold, found, err := unstructured.NestedInt64(resource.Object, "spec", "consecutiveFailuresThreshold")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(threshold).To(Equal(int64(3)))

			checks, found, err := unstructured.NestedSlice(resource.Object, "spec", "checks")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
			Expect(checks).To(BeEmpty())
		})

		It("requires an explicit host and port in TCPListener and Both", func() {
			config := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, config)).To(Succeed())
			for _, mode := range []nodenetworkcheckoperatorv1alpha1.NetworkDetectionMode{
				nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeTCPListener,
				nodenetworkcheckoperatorv1alpha1.NetworkDetectionModeBoth,
			} {
				config.Spec.NetworkDetectionMode = &mode
				for _, checks := range [][]nodenetworkcheckoperatorv1alpha1.NetworkCheck{
					nil,
					{{Type: testTCPCheck.Type, Port: 443}},
					{{Type: testTCPCheck.Type, Host: testTCPCheck.Host}},
					{{Type: testTCPCheck.Type, Host: testTCPCheck.Host, Port: 65536}},
					{testTCPCheck, testTCPCheck},
				} {
					candidate := config.DeepCopy()
					candidate.Spec.Checks = checks
					Expect(errors.IsInvalid(k8sClient.Update(ctx, candidate))).To(BeTrue())
				}
				config.Spec.Checks = []nodenetworkcheckoperatorv1alpha1.NetworkCheck{testTCPCheck}
				Expect(k8sClient.Update(ctx, config)).To(Succeed())
				config.Spec.Checks = nil
			}
		})

		It("accepts positive settings and rejects zero or negative values", func() {
			config := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, config)).To(Succeed())

			for _, invalid := range []int32{0, -1} {
				candidate := config.DeepCopy()
				candidate.Spec.NetworkCheckIntervalSeconds = &invalid
				Expect(errors.IsInvalid(k8sClient.Update(ctx, candidate))).To(BeTrue())

				candidate = config.DeepCopy()
				candidate.Spec.ConsecutiveFailuresThreshold = &invalid
				Expect(errors.IsInvalid(k8sClient.Update(ctx, candidate))).To(BeTrue())
			}

			minimum := int32(1)
			config.Spec.NetworkCheckIntervalSeconds = &minimum
			config.Spec.ConsecutiveFailuresThreshold = &minimum
			Expect(k8sClient.Update(ctx, config)).To(Succeed())
		})
	})
})
