# Network-Based Node Health Detection POC

**Inspired by RHWA-450** â€” Alternative methods for detecting unhealthy nodes in OpenShift clusters.

## Overview

Kubernetes determines node health primarily through **Node Conditions** reported by the kubelet. When a node becomes truly unresponsive (kernel hang, network partition, hardware failure), the kubelet can no longer report its own status â€” the control plane must wait for a lease timeout before marking the node `NotReady`. This delay can be 40 seconds or more.

This POC demonstrates **network-based health detection** as a complementary signal: using active TCP probes to determine whether a node is reachable, independent of kubelet self-reporting. Two approaches are explored:

1. **TCP Handshake Monitoring** â€” Probe a dedicated listener on each node (port 19100)
2. **Existing Service Monitoring** â€” Probe the kubelet port (10250) already running on each node

## Architecture

```
 OpenShift Cluster
+-----------------------------------------------------------------------+
|                                                                       |
|  Namespace: network-health-poc                                        |
|                                                                       |
|  +---------------------+    TCP probe     +------------------------+  |
|  | handshake-monitor   | ----:19100-----> | tcp-listener (node-1)  |  |
|  | (Pod)               | ----:19100-----> | tcp-listener (node-2)  |  |
|  |                     | ----:19100-----> | tcp-listener (node-3)  |  |
|  | nc -z -w 2 IP 19100 |                  | nc -l -p 19100         |  |
|  +---------------------+                  | (DaemonSet, hostNet)   |  |
|                                           +------------------------+  |
|  +---------------------+    TCP probe                                 |
|  | service-monitor     | ----:10250-----> kubelet (node-1)            |
|  | (Pod)               | ----:10250-----> kubelet (node-2)            |
|  |                     | ----:10250-----> kubelet (node-3)            |
|  | nc -z -w 2 IP 10250 |                 (already running)           |
|  +---------------------+                                             |
|                                                                       |
+-----------------------------------------------------------------------+
```

**Key components:**

| Component | Type | Network | Purpose |
|-----------|------|---------|---------|
| `tcp-listener` | DaemonSet | hostNetwork | Listens on port 19100 on each worker node |
| `handshake-monitor` | Pod | cluster network | Probes port 19100 on every worker node |
| `service-monitor` | Pod | cluster network | Probes kubelet port 10250 on every worker node |

## Prerequisites

- **OpenShift cluster** (4.x) with worker nodes
- **`oc` CLI** installed and authenticated (`oc login`)
- **Cluster-admin** or ability to:
  - Create namespaces
  - Create a ClusterRole and ClusterRoleBinding for listing nodes
  - Grant `hostnetwork-v2` (or `hostnetwork`) SCC to a ServiceAccount
- **Network access** from pods to worker node IPs on ports 19100 and 10250

## Quick Start

```bash
# 1. Deploy listener agents on all worker nodes
./network-health-poc.sh setup-listeners

# 2. Start the handshake monitor (probes listeners on :19100)
./network-health-poc.sh setup-handshake-monitor

# 3. Or start the service monitor (probes kubelet on :10250, no listeners needed)
./network-health-poc.sh setup-service-monitor

# Check what's running
./network-health-poc.sh status

# Tail monitor logs
./network-health-poc.sh logs handshake
./network-health-poc.sh logs service

# Clean up everything
./network-health-poc.sh teardown
```

## Commands

### `setup-listeners`

Deploys the infrastructure for TCP handshake monitoring:

1. Creates the `network-health-poc` namespace
2. Creates a ServiceAccount and binds it to the `hostnetwork-v2` SCC
3. Deploys a DaemonSet that runs a busybox TCP listener (`nc -l -p 19100`) on every worker node using `hostNetwork: true`

```bash
./network-health-poc.sh setup-listeners
```

### `setup-handshake-monitor`

Deploys a single pod that continuously probes port 19100 on every worker node:

- Auto-discovers worker node IPs via the Kubernetes API
- Runs `nc -z -w 2 <node-ip> 19100` every 5 seconds
- Logs `HEALTHY` or `UNHEALTHY` with timestamps

**Requires:** Listener pods from `setup-listeners` must be running.

```bash
./network-health-poc.sh setup-handshake-monitor
```

### `setup-service-monitor`

Deploys a single pod that continuously probes the kubelet on port 10250:

- Auto-discovers worker node IPs via the Kubernetes API
- Runs `nc -z -w 2 <node-ip> 10250` every 5 seconds
- Logs `HEALTHY` or `UNHEALTHY` with timestamps

**Does NOT require listener pods** â€” it checks an existing service (kubelet).

```bash
./network-health-poc.sh setup-service-monitor
```

### `status`

Shows the current state of all POC resources:

```bash
./network-health-poc.sh status
```

### `logs`

Tails the logs of a monitor pod in real time:

```bash
./network-health-poc.sh logs handshake   # TCP handshake monitor
./network-health-poc.sh logs service     # Kubelet service monitor
```

### `teardown`

Removes all POC resources:

1. Removes SCC bindings and monitor node-read permissions
2. Deletes the `network-health-poc` namespace (cascades to all resources)

```bash
./network-health-poc.sh teardown
```

## How It Works

### TCP Three-Way Handshake Detection

The TCP three-way handshake is the foundation of every TCP connection:

```
Monitor Pod           Worker Node (port 19100)
     |                        |
     |-------- SYN ---------> |   1. Monitor initiates connection
     |<------ SYN-ACK ------- |   2. Listener acknowledges
     |-------- ACK ---------> |   3. Connection established
     |                        |
     |-------- FIN ---------> |   4. Monitor closes immediately
```

`nc -z -w 2` performs exactly this sequence with a 2-second timeout. If the handshake completes, the node's network stack is functional. If it times out, the node may be:
- Network-partitioned
- Kernel-hung (unable to process interrupts)
- Experiencing NIC failure
- Powered off

This catches failures that kubelet self-reporting cannot detect, because the probe originates externally.

### Existing Service (Kubelet) Detection

The kubelet exposes an HTTPS API on port 10250 on every node. The service monitor uses the same `nc -z` technique but targets this existing port instead of a dedicated listener:

```
Monitor Pod           Worker Node (port 10250)
     |                        |
     |-------- SYN ---------> |   TCP connect to kubelet port
     |<------ SYN-ACK ------- |   Kubelet's TCP stack responds
     |-------- ACK ---------> |   Connection established
     |-------- FIN ---------> |   Close (no HTTP request sent)
```

This confirms the node's network stack AND that the kubelet process is running and accepting connections.

## Verifying It Works

### Check health logs

```bash
# Watch handshake monitor output
./network-health-poc.sh logs handshake

# Expected output:
# [2024-01-15 10:30:05] Node 10.0.1.10: HEALTHY (TCP handshake on port 19100 succeeded)
# [2024-01-15 10:30:05] Node 10.0.1.11: HEALTHY (TCP handshake on port 19100 succeeded)
# [2024-01-15 10:30:05] Node 10.0.1.12: HEALTHY (TCP handshake on port 19100 succeeded)
```

### Simulate a failure

To test unhealthy detection, you can cordon and drain a node, or (in a test cluster) stop the listener:

```bash
# Option 1: Delete a listener pod to simulate listener failure
oc delete pod -n network-health-poc -l component=tcp-listener \
    --field-selector spec.nodeName=<node-name>

# The handshake monitor should report UNHEALTHY for that node within 5 seconds

# Option 2: For service monitor, cordon a node (kubelet stays running but
# this demonstrates the monitoring flow)
oc adm cordon <node-name>
```

### Check resource status

```bash
./network-health-poc.sh status
```

## Comparison of Approaches

| Aspect | TCP Handshake (port 19100) | Kubelet Service (port 10250) |
|--------|--------------------------|------------------------------|
| **What it detects** | Node network reachability | Node reachability + kubelet process health |
| **Extra pods needed** | Yes (DaemonSet listeners) | No (uses existing kubelet) |
| **SCC requirements** | `hostnetwork-v2` for listeners | None (monitor uses restricted SCC) |
| **Failure granularity** | Network-level only | Network + process level |
| **False positives** | If listener pod crashes (not node failure) | If kubelet restarts (brief window) |
| **Simplicity** | More moving parts | Simpler (no listener infrastructure) |
| **Independence** | Fully independent of kubelet | Coupled to kubelet availability |
| **Customizability** | Can check any custom port/service | Fixed to kubelet port |

**Recommendation:**
- Use **kubelet service monitoring** for a quick, low-overhead health signal
- Use **TCP handshake monitoring** when you need a health check independent of any Kubernetes component, or when you want to monitor custom application ports

## Cleanup

```bash
./network-health-poc.sh teardown
```

This removes:
- All pods (listeners and monitors)
- The DaemonSet
- The ServiceAccounts and monitor node-read permissions
- SCC bindings
- The `network-health-poc` namespace

## Future Improvements

- **Alerting integration**: Send alerts via webhook when nodes are detected as unhealthy
- **Prometheus metrics**: Expose probe results as Prometheus metrics for Grafana dashboards
- **Configurable thresholds**: Mark a node unhealthy only after N consecutive failures
- **Keep-alive monitoring**: Maintain persistent TCP connections and detect drops (vs. periodic probes)
- **Multi-port probing**: Check multiple services per node (kubelet, CRI-O, API server on control plane)
- **Node remediation**: Integrate with Machine Health Check or Node Health Check Operator to trigger automatic remediation (drain, reboot, or replace)
- **Operator packaging**: Package as a Kubernetes Operator with CRDs for configuration
- **mTLS probes**: Use mutual TLS for authenticated health checks
- **Cross-subnet detection**: Deploy monitors in different network segments to detect partial network partitions
