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
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

const (
	kubeletPort          = "10250"
	serviceProbeTimeout  = 2 * time.Second
	maxConcurrentProbes  = 32
	defaultCheckInterval = 5 * time.Second
	defaultFailureLimit  = int32(3)
	serviceFailureReason = "KubeletServiceUnreachable"
)

var (
	errNoNodeAddress        = errors.New("node has no valid IP address")
	errConfigurationChanged = errors.New("network check configuration changed")
)

type configVersion struct {
	uid        types.UID
	generation int64
}

type serviceCheckState struct {
	uid       types.UID
	lastCheck time.Time
	failures  int32
	reported  bool
	addresses string
}

// serviceProbeFunc checks a single IP address. It returns nil when the kubelet
// TCP port is reachable.
type serviceProbeFunc func(context.Context, string) error

type serviceSettings struct {
	interval  time.Duration
	threshold int32
	mode      networkv1alpha1.NetworkDetectionMode
}

// serviceConditionCleaner handles configurations deleted while the operator
// was stopped, when no config event is available to trigger reconciliation.
type serviceConditionCleaner struct {
	reconciler *NodeNetworkCheckOperatorConfigReconciler
}

func (*serviceConditionCleaner) NeedLeaderElection() bool { return true }

func (c *serviceConditionCleaner) Start(ctx context.Context) error {
	logger := ctrl.Log.WithName("service-condition-cleaner")
	for {
		active, _, err := activeConfiguration(ctx, c.reconciler.reader())
		if err == nil && active != nil {
			return nil
		}
		if err == nil {
			var result ctrl.Result
			result, err = c.reconciler.clearServiceConditions(ctx, nil)
			if err == nil && !result.Requeue {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			logger.Error(err, "Failed to clean up stale network service conditions")
		}
		timer := time.NewTimer(defaultCheckInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func settingsFor(config *networkv1alpha1.NodeNetworkCheckOperatorConfig) (serviceSettings, error) {
	settings := serviceSettings{
		interval:  defaultCheckInterval,
		threshold: defaultFailureLimit,
		mode:      networkv1alpha1.NetworkDetectionModeService,
	}
	if config.Spec.NetworkCheckIntervalSeconds != nil {
		settings.interval = time.Duration(*config.Spec.NetworkCheckIntervalSeconds) * time.Second
	}
	if config.Spec.ConsecutiveFailuresThreshold != nil {
		settings.threshold = *config.Spec.ConsecutiveFailuresThreshold
	}
	if config.Spec.NetworkDetectionMode != nil {
		settings.mode = *config.Spec.NetworkDetectionMode
	}
	if settings.interval <= 0 || settings.threshold < 1 {
		return settings, fmt.Errorf("network check interval and failure threshold must be positive")
	}
	switch settings.mode {
	case networkv1alpha1.NetworkDetectionModeService, networkv1alpha1.NetworkDetectionModeTCPListener, networkv1alpha1.NetworkDetectionModeBoth:
		return settings, nil
	default:
		return settings, fmt.Errorf("unsupported network detection mode %q", settings.mode)
	}
}

// activeConfiguration selects one cluster-wide configuration so multiple CRs
// cannot independently assert conflicting conditions on the same nodes.
func activeConfiguration(ctx context.Context, reader client.Reader) (*networkv1alpha1.NodeNetworkCheckOperatorConfig, int, error) {
	configs := &networkv1alpha1.NodeNetworkCheckOperatorConfigList{}
	if err := reader.List(ctx, configs); err != nil {
		return nil, 0, err
	}
	active := make([]networkv1alpha1.NodeNetworkCheckOperatorConfig, 0, len(configs.Items))
	for _, config := range configs.Items {
		if config.DeletionTimestamp.IsZero() {
			active = append(active, config)
		}
	}
	if len(active) == 0 {
		return nil, 0, nil
	}
	sort.Slice(active, func(i, j int) bool {
		if !active[i].CreationTimestamp.Equal(&active[j].CreationTimestamp) {
			return active[i].CreationTimestamp.Before(&active[j].CreationTimestamp)
		}
		return client.ObjectKeyFromObject(&active[i]).String() < client.ObjectKeyFromObject(&active[j]).String()
	})
	return &active[0], len(active), nil
}

func (r *NodeNetworkCheckOperatorConfigReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *NodeNetworkCheckOperatorConfigReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *NodeNetworkCheckOperatorConfigReconciler) configurationMatches(ctx context.Context, expected *networkv1alpha1.NodeNetworkCheckOperatorConfig) (bool, error) {
	active, _, err := activeConfiguration(ctx, r.reader())
	if err != nil {
		return false, err
	}
	if expected == nil {
		return active == nil, nil
	}
	return active != nil && client.ObjectKeyFromObject(active) == client.ObjectKeyFromObject(expected) &&
		active.UID == expected.UID && active.Generation == expected.Generation, nil
}

func (r *NodeNetworkCheckOperatorConfigReconciler) reconcileDeletedConfig(ctx context.Context) (ctrl.Result, error) {
	active, _, err := activeConfiguration(ctx, r.reader())
	if err != nil {
		return ctrl.Result{}, err
	}
	if active != nil {
		return ctrl.Result{}, nil
	}
	r.activeConfig = ""
	r.serviceChecks = nil
	return r.clearServiceConditions(ctx, nil)
}

func (r *NodeNetworkCheckOperatorConfigReconciler) reconcileServiceChecks(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	settings, err := settingsFor(config)
	if err != nil {
		return ctrl.Result{}, err
	}
	active, count, err := activeConfiguration(ctx, r.reader())
	if err != nil {
		return ctrl.Result{}, err
	}
	if active == nil || client.ObjectKeyFromObject(active) != client.ObjectKeyFromObject(config) || active.UID != config.UID {
		// A non-active config polls so it can take over after the active one is deleted.
		if active != nil {
			key := client.ObjectKeyFromObject(config)
			if r.inactiveConfigs == nil {
				r.inactiveConfigs = make(map[types.NamespacedName]types.UID)
			}
			if previous, ok := r.inactiveConfigs[key]; !ok || previous != active.UID {
				logger.Info("Skipping configuration because another configuration is active", "activeConfig", client.ObjectKeyFromObject(active))
				r.inactiveConfigs[key] = active.UID
			}
		}
		return ctrl.Result{RequeueAfter: defaultCheckInterval}, nil
	}
	delete(r.inactiveConfigs, client.ObjectKeyFromObject(config))
	if active.Generation != config.Generation {
		return ctrl.Result{Requeue: true}, nil
	}
	if count > 1 {
		logger.V(1).Info("Using the oldest network check configuration", "activeConfig", client.ObjectKeyFromObject(active), "configCount", count)
	}
	if r.activeConfig != config.UID || r.serviceChecks == nil {
		r.activeConfig = config.UID
		r.serviceChecks = make(map[string]*serviceCheckState)
	}
	if settings.mode == networkv1alpha1.NetworkDetectionModeTCPListener {
		r.serviceChecks = nil
		return r.clearServiceConditions(ctx, config)
	}
	return r.checkNodeServices(ctx, config, settings)
}

func (r *NodeNetworkCheckOperatorConfigReconciler) clearServiceConditions(ctx context.Context, expected *networkv1alpha1.NodeNetworkCheckOperatorConfig) (ctrl.Result, error) {
	nodes := &corev1.NodeList{}
	if err := r.reader().List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	var failures []error
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if serviceCondition(node) == nil {
			continue
		}
		matches, err := r.configurationMatches(ctx, expected)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !matches {
			return ctrl.Result{Requeue: true}, nil
		}
		if err := r.updateServiceCondition(ctx, node, false, 0, expected, false); err != nil {
			if errors.Is(err, errConfigurationChanged) {
				return ctrl.Result{Requeue: true}, nil
			}
			failures = append(failures, err)
		}
	}
	return ctrl.Result{}, errors.Join(failures...)
}

type serviceCheckResult struct {
	node      corev1.Node
	startedAt time.Time
	err       error
}

func (r *NodeNetworkCheckOperatorConfigReconciler) checkNodeServices(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, settings serviceSettings) (ctrl.Result, error) {
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return ctrl.Result{}, err
	}
	now := r.clock()
	seen := make(map[string]bool, len(nodes.Items))
	var due []corev1.Node
	for _, node := range nodes.Items {
		seen[node.Name] = true
		addresses := strings.Join(nodeAddresses(&node), ",")
		state := r.serviceChecks[node.Name]
		if state == nil || state.uid != node.UID {
			state = &serviceCheckState{uid: node.UID, addresses: addresses}
			r.serviceChecks[node.Name] = state
		} else if state.addresses != addresses {
			state.addresses = addresses
			state.failures = 0
			state.lastCheck = time.Time{}
		}
		if state.lastCheck.IsZero() || !now.Before(state.lastCheck.Add(settings.interval)) {
			due = append(due, node)
		}
	}
	for name := range r.serviceChecks {
		if !seen[name] {
			delete(r.serviceChecks, name)
		}
	}

	results := make(chan serviceCheckResult, len(due))
	semaphore := make(chan struct{}, maxConcurrentProbes)
	var workers sync.WaitGroup
	for _, node := range due {
		workers.Add(1)
		go func(node corev1.Node) {
			defer workers.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}
			started := r.clock()
			results <- serviceCheckResult{node: node, startedAt: started, err: r.checkKubelet(ctx, &node)}
		}(node)
	}
	workers.Wait()
	close(results)
	if ctx.Err() != nil {
		return ctrl.Result{}, ctx.Err()
	}
	completed := make([]serviceCheckResult, 0, len(due))
	for result := range results {
		r.serviceChecks[result.node.Name].lastCheck = result.startedAt
		completed = append(completed, result)
	}
	if len(due) > 0 {
		matches, err := r.configurationMatches(ctx, config)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !matches {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	logger := log.FromContext(ctx)
	var failures []error
	for _, result := range completed {
		state := r.serviceChecks[result.node.Name]
		if errors.Is(result.err, errNoNodeAddress) {
			state.failures = 0
			logger.Info("Skipping node without a usable IP address", "node", result.node.Name)
			continue
		}
		if result.err != nil {
			if state.failures < settings.threshold {
				state.failures++
			}
			logger.V(1).Info("Kubelet service check failed", "node", result.node.Name, "failures", state.failures, "error", result.err)
			if state.failures >= settings.threshold && !serviceConditionTrue(&result.node) {
				if err := r.updateServiceCondition(ctx, &result.node, true, settings.threshold, config, true); err != nil {
					if errors.Is(err, errConfigurationChanged) {
						return ctrl.Result{Requeue: true}, nil
					}
					failures = append(failures, err)
				} else {
					state.reported = true
				}
			}
			continue
		}
		state.failures = 0
		if state.reported || serviceCondition(&result.node) != nil {
			if err := r.updateServiceCondition(ctx, &result.node, false, 0, config, true); err != nil {
				if errors.Is(err, errConfigurationChanged) {
					return ctrl.Result{Requeue: true}, nil
				}
				failures = append(failures, err)
			} else {
				state.reported = false
			}
		}
	}
	if err := errors.Join(failures...); err != nil {
		return ctrl.Result{}, err
	}

	next := settings.interval
	now = r.clock()
	for _, state := range r.serviceChecks {
		remaining := state.lastCheck.Add(settings.interval).Sub(now)
		if remaining < next {
			next = remaining
		}
	}
	if next <= 0 {
		next = time.Millisecond
	}
	return ctrl.Result{RequeueAfter: next}, nil
}

func nodeAddresses(node *corev1.Node) []string {
	internal, external := []string{}, []string{}
	seenInternal, seenExternal := make(map[string]bool), make(map[string]bool)
	for _, address := range node.Status.Addresses {
		if net.ParseIP(address.Address) == nil {
			continue
		}
		switch address.Type {
		case corev1.NodeInternalIP:
			if !seenInternal[address.Address] {
				internal = append(internal, address.Address)
				seenInternal[address.Address] = true
			}
		case corev1.NodeExternalIP:
			if !seenExternal[address.Address] {
				external = append(external, address.Address)
				seenExternal[address.Address] = true
			}
		}
	}
	if len(internal) > 0 {
		return internal
	}
	return external
}

func (r *NodeNetworkCheckOperatorConfigReconciler) checkKubelet(ctx context.Context, node *corev1.Node) error {
	addresses := nodeAddresses(node)
	if len(addresses) == 0 {
		return errNoNodeAddress
	}
	probe := r.probeService
	if probe == nil {
		probe = probeKubeletService
	}
	var failures []error
	for _, address := range addresses {
		if err := probe(ctx, address); err == nil {
			return nil
		} else {
			failures = append(failures, fmt.Errorf("%s: %w", address, err))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return errors.Join(failures...)
}

func probeKubeletService(ctx context.Context, address string) error {
	dialer := net.Dialer{Timeout: serviceProbeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(address, kubeletPort))
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func serviceCondition(node *corev1.Node) *corev1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == networkv1alpha1.NodeConditionServiceUnhealthy {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

func serviceConditionTrue(node *corev1.Node) bool {
	condition := serviceCondition(node)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func (r *NodeNetworkCheckOperatorConfigReconciler) updateServiceCondition(ctx context.Context, sampledNode *corev1.Node, unhealthy bool, threshold int32, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, verifyAddress bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		matches, err := r.configurationMatches(ctx, config)
		if err != nil {
			return err
		}
		if !matches {
			return errConfigurationChanged
		}
		node := &corev1.Node{}
		if err := r.reader().Get(ctx, client.ObjectKey{Name: sampledNode.Name}, node); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("get node %s: %w", sampledNode.Name, err)
		}
		if node.UID != sampledNode.UID {
			return nil
		}
		if verifyAddress && !slices.Equal(nodeAddresses(node), nodeAddresses(sampledNode)) {
			return nil
		}
		condition := serviceCondition(node)
		if unhealthy {
			if condition != nil && condition.Status == corev1.ConditionTrue {
				return nil
			}
			now := metav1.NewTime(r.clock())
			updated := corev1.NodeCondition{
				Type:               networkv1alpha1.NodeConditionServiceUnhealthy,
				Status:             corev1.ConditionTrue,
				Reason:             serviceFailureReason,
				Message:            fmt.Sprintf("Kubelet service was unreachable for %d consecutive network checks", threshold),
				LastHeartbeatTime:  now,
				LastTransitionTime: now,
			}
			if condition == nil {
				node.Status.Conditions = append(node.Status.Conditions, updated)
			} else {
				*condition = updated
			}
		} else {
			if condition == nil {
				return nil
			}
			conditions := make([]corev1.NodeCondition, 0, len(node.Status.Conditions)-1)
			for _, existing := range node.Status.Conditions {
				if existing.Type != networkv1alpha1.NodeConditionServiceUnhealthy {
					conditions = append(conditions, existing)
				}
			}
			node.Status.Conditions = conditions
		}
		if err := r.Status().Update(ctx, node); err != nil {
			return fmt.Errorf("update network service condition for node %s: %w", node.Name, err)
		}
		log.FromContext(ctx).Info("Updated network service condition", "node", node.Name, "unhealthy", unhealthy)
		return nil
	})
}
