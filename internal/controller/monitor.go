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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	networkv1alpha1 "github.com/mshitrit/node-network-check-operator/api/v1alpha1"
)

// MonitorKind identifies a monitor workload and the node condition it owns.
type MonitorKind string

const (
	ServiceMonitor MonitorKind = "service-monitor"
	TCPMonitor     MonitorKind = "tcp-monitor"

	tcpProbeTimeout = time.Second
)

func (m MonitorKind) enabled(mode networkv1alpha1.NetworkDetectionMode) bool {
	return mode == networkv1alpha1.NetworkDetectionModeBoth ||
		(m == ServiceMonitor && mode == networkv1alpha1.NetworkDetectionModeService) ||
		(m == TCPMonitor && mode == networkv1alpha1.NetworkDetectionModeTCPListener)
}

func (m MonitorKind) condition() corev1.NodeConditionType {
	if m == TCPMonitor {
		return networkv1alpha1.NodeConditionTCPListenerUnhealthy
	}
	return networkv1alpha1.NodeConditionServiceUnhealthy
}

func (m MonitorKind) endpoint() string {
	if m == TCPMonitor {
		return "TCP endpoint"
	}
	return "Kubelet service"
}

func (m MonitorKind) failureReason() string {
	if m == TCPMonitor {
		return "TCPEndpointUnreachable"
	}
	return serviceFailureReason
}

// RunMonitor runs the configured check while the pod's configuration is active.
func RunMonitor(ctx context.Context, c client.Client, monitor MonitorKind, key client.ObjectKey, version, nodeName string) error {
	if monitor != ServiceMonitor && monitor != TCPMonitor {
		return fmt.Errorf("unknown monitor %q", monitor)
	}
	if key.Name == "" || key.Namespace == "" || version == "" {
		return fmt.Errorf("monitor requires a configuration name, namespace, and version")
	}
	if monitor == TCPMonitor && nodeName == "" {
		return fmt.Errorf("TCP monitor requires a node name")
	}
	logger := log.FromContext(ctx).WithName(string(monitor))
	ctx = log.IntoContext(ctx, logger)
	r := &NodeNetworkCheckOperatorConfigReconciler{
		Client:        c,
		serviceChecks: make(map[string]*serviceCheckState),
	}
	var tcpFailures int32
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
		if monitor == TCPMonitor {
			if settings.tcpCheck == nil {
				return fmt.Errorf("TCP monitor requires one TCP endpoint in checks")
			}
			started := time.Now()
			if err := r.checkLocalTCP(ctx, config, settings, nodeName, &tcpFailures); err != nil && ctx.Err() == nil {
				logger.Error(err, "Check TCP endpoint", "node", nodeName)
			}
			next = max(settings.interval-time.Since(started), time.Millisecond)
			continue
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

func (r *NodeNetworkCheckOperatorConfigReconciler) checkLocalTCP(ctx context.Context, config *networkv1alpha1.NodeNetworkCheckOperatorConfig, settings networkCheckSettings, nodeName string, failures *int32) error {
	node := &corev1.Node{}
	if err := r.reader().Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	check := settings.tcpCheck
	probeErr := probeTCP(ctx, check.Host, int(check.Port), tcpProbeTimeout)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	condition := nodeCondition(node, TCPMonitor.condition())
	if probeErr == nil {
		*failures = 0
		if condition != nil {
			return r.updateMonitorCondition(ctx, node, TCPMonitor, false, 0, config, false)
		}
		return nil
	}

	if *failures < settings.threshold {
		*failures++
	}
	log.FromContext(ctx).V(1).Info("TCP endpoint check failed",
		"node", nodeName, "host", check.Host, "port", check.Port, "failures", *failures, "error", probeErr)
	if *failures >= settings.threshold && (condition == nil || condition.Status != corev1.ConditionTrue) {
		return r.updateMonitorCondition(ctx, node, TCPMonitor, true, settings.threshold, config, false)
	}
	return nil
}
