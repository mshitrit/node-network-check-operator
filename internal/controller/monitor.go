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
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

// MonitorKind identifies a monitor workload and the node condition it owns.
type MonitorKind string

const (
	ServiceMonitor   MonitorKind = "service-monitor"
	HandshakeMonitor MonitorKind = "handshake-monitor"
)

func (m MonitorKind) enabled(mode networkv1alpha1.NetworkDetectionMode) bool {
	return mode == networkv1alpha1.NetworkDetectionModeBoth ||
		(m == ServiceMonitor && mode == networkv1alpha1.NetworkDetectionModeService) ||
		(m == HandshakeMonitor && mode == networkv1alpha1.NetworkDetectionModeTCPListener)
}

func (m MonitorKind) condition() corev1.NodeConditionType {
	if m == HandshakeMonitor {
		return networkv1alpha1.NodeConditionTCPListenerUnhealthy
	}
	return networkv1alpha1.NodeConditionServiceUnhealthy
}

func (m MonitorKind) endpoint() string {
	if m == HandshakeMonitor {
		return "TCP listener"
	}
	return "Kubelet service"
}

func (m MonitorKind) failureReason() string {
	if m == HandshakeMonitor {
		return "TCPListenerUnreachable"
	}
	return serviceFailureReason
}

// RunMonitor checks nodes while the configuration used for this pod is active.
func RunMonitor(ctx context.Context, c client.Client, monitor MonitorKind, key client.ObjectKey, version, namespace string) error {
	if monitor != ServiceMonitor && monitor != HandshakeMonitor {
		return fmt.Errorf("unknown monitor %q", monitor)
	}
	if key.Name == "" || key.Namespace == "" || version == "" || namespace == "" {
		return fmt.Errorf("monitor requires a configuration name, namespace, version, and workload namespace")
	}
	logger := log.FromContext(ctx).WithName(string(monitor))
	ctx = log.IntoContext(ctx, logger)
	r := &NodeNetworkCheckOperatorConfigReconciler{
		Client:            c,
		monitor:           monitor,
		listenerNamespace: namespace,
		serviceChecks:     make(map[string]*serviceCheckState),
	}
	next := time.Duration(0)
	for ctx.Err() == nil {
		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		next = defaultCheckInterval
		config, _, err := activeConfiguration(ctx, c)
		if err != nil {
			logger.Error(err, "Read active network check configuration")
			continue
		}
		if config == nil || client.ObjectKeyFromObject(config) != key || workloadVersion(config) != version {
			// Stop publishing results immediately. The manager will remove this pod.
			<-ctx.Done()
			return nil
		}
		settings, err := settingsFor(config)
		if err != nil {
			return err
		}
		if !monitor.enabled(settings.mode) {
			return fmt.Errorf("%s is disabled in %s mode", monitor, settings.mode)
		}
		result, err := r.checkNodes(ctx, config, settings)
		if err != nil {
			logger.Error(err, "Check node reachability")
		} else if result.Requeue {
			next = 0
		} else {
			next = result.RequeueAfter
		}
	}
	return nil
}

func (r *NodeNetworkCheckOperatorConfigReconciler) readyListeners(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig) (map[string]bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(r.listenerNamespace), client.MatchingLabels(workloadLabels(listenerName))); err != nil {
		return nil, err
	}
	ready := make(map[string]bool)
	for _, pod := range pods.Items {
		if pod.Annotations[configVersionKey] != workloadVersion(config) || pod.DeletionTimestamp != nil {
			continue
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready[pod.Spec.NodeName] = true
				break
			}
		}
	}
	return ready, nil
}

func validListenerPort(port int) bool {
	return port >= 1024 && port <= 65535 && port != kubeletPort
}

// RunTCPListener accepts and closes connections without reading application data.
func RunTCPListener(ctx context.Context, port int) error {
	if !validListenerPort(port) {
		return fmt.Errorf("invalid TCP listener port %d", port)
	}
	cfg := net.ListenConfig{}
	listener, err := cfg.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()

	log.FromContext(ctx).Info("Listening for TCP checks", "port", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		_ = conn.Close()
	}
}
