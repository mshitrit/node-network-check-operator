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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nodenetworkcheckoperatorv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

// NodeNetworkCheckOperatorConfigReconciler reconciles a NodeNetworkCheckOperatorConfig object
type NodeNetworkCheckOperatorConfigReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Workloads *WorkloadSettings

	serviceChecks     map[string]*serviceCheckState
	activeConfig      configVersion
	loggedConfigs     map[types.NamespacedName]configVersion
	inactiveConfigs   map[types.NamespacedName]types.UID
	probeService      serviceProbeFunc
	now               func() time.Time
	monitor           MonitorKind
	listenerNamespace string
}

//+kubebuilder:rbac:groups=node-network-check-operator.medik8s.io,resources=nodenetworkcheckoperatorconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=node-network-check-operator.medik8s.io,resources=nodenetworkcheckoperatorconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=node-network-check-operator.medik8s.io,resources=nodenetworkcheckoperatorconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes/status,verbs=update
//+kubebuilder:rbac:groups="",resources=pods,verbs=list
//+kubebuilder:rbac:groups=apps,resources=daemonsets;deployments,verbs=get;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NodeNetworkCheckOperatorConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *NodeNetworkCheckOperatorConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("config", req.NamespacedName)

	config := &nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}
	if err := r.Get(ctx, req.NamespacedName, config); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Configuration no longer exists")
			delete(r.loggedConfigs, req.NamespacedName)
			delete(r.inactiveConfigs, req.NamespacedName)
			return r.reconcileDeletedConfig(ctx)
		}
		return ctrl.Result{}, err
	}

	version := configVersion{uid: config.UID, generation: config.Generation}
	if r.loggedConfigs == nil {
		r.loggedConfigs = make(map[types.NamespacedName]configVersion)
	}
	if previous, ok := r.loggedConfigs[req.NamespacedName]; !ok || previous != version {
		logger.Info("Reconciling configuration",
			"networkDetectionMode", config.Spec.NetworkDetectionMode,
			"tcpListenerPort", config.Spec.TCPListenerPort,
			"networkCheckIntervalSeconds", config.Spec.NetworkCheckIntervalSeconds,
			"consecutiveFailuresThreshold", config.Spec.ConsecutiveFailuresThreshold,
		)
		r.loggedConfigs[req.NamespacedName] = version
	}

	return r.reconcileNetworkChecks(ctx, config)
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeNetworkCheckOperatorConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(&staleChecksCleaner{reconciler: r}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nodenetworkcheckoperatorv1alpha1.NodeNetworkCheckOperatorConfig{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
