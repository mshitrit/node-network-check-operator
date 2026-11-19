#!/usr/bin/env bash
#
# network-health-poc.sh â€” Network-Based Node Health Detection POC for OpenShift
# Inspired by RHWA-450: Alternative methods for detecting unhealthy nodes
#
# Usage: ./network-health-poc.sh <command>
# Commands: setup-listeners, setup-handshake-monitor, setup-service-monitor,
#           teardown, status, logs <handshake|service>

set -euo pipefail

###############################################################################
# Constants
###############################################################################
NAMESPACE="network-health-poc"
SA_NAME="network-health-sa"
MONITOR_SA_NAME="network-health-monitor"
MONITOR_ROLE_NAME="${NAMESPACE}-node-reader"
DAEMONSET_NAME="tcp-listener"
HANDSHAKE_MONITOR_NAME="handshake-monitor"
SERVICE_MONITOR_NAME="service-monitor"
LISTENER_PORT=19100
BUSYBOX_IMAGE="docker.io/library/busybox:latest"

###############################################################################
# Color helpers
###############################################################################
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; }

###############################################################################
# Prerequisite checks
###############################################################################
check_oc() {
    if ! command -v oc &>/dev/null; then
        error "oc CLI not found. Please install the OpenShift CLI."
        exit 1
    fi
    if ! oc whoami &>/dev/null; then
        error "Not logged in to an OpenShift cluster. Run 'oc login' first."
        exit 1
    fi
    info "Logged in as $(oc whoami) on $(oc whoami --show-server 2>/dev/null || echo 'unknown server')"
}

namespace_exists() {
    oc get namespace "$NAMESPACE" &>/dev/null
}

###############################################################################
# Shared monitor setup
###############################################################################
setup_monitor_access() {
    info "Configuring node discovery access for monitor pods..."
    oc apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${MONITOR_SA_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: network-health-poc
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${MONITOR_ROLE_NAME}
  labels:
    app: network-health-poc
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${MONITOR_ROLE_NAME}
  labels:
    app: network-health-poc
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${MONITOR_ROLE_NAME}
subjects:
  - kind: ServiceAccount
    name: ${MONITOR_SA_NAME}
    namespace: ${NAMESPACE}
EOF
}

# Emit the same discovery function into both monitor pod commands.
node_discovery_script() {
    cat <<'EOF'
          discover_node_ips() {
            wget -qO- https://kubernetes.default.svc/api/v1/nodes \
              --header "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
              --no-check-certificate 2>/dev/null | \
              grep -o '"address"[[:space:]]*:[[:space:]]*"[0-9.]*"' | \
              grep -o '[0-9.]*' | sort -u
          }
EOF
}

###############################################################################
# setup-listeners: Deploy TCP listener DaemonSet on all nodes
###############################################################################
cmd_setup_listeners() {
    check_oc

    info "Setting up TCP listener pods on all nodes..."

    # Create namespace
    if namespace_exists; then
        info "Namespace '$NAMESPACE' already exists."
    else
        info "Creating namespace '$NAMESPACE'..."
        oc create namespace "$NAMESPACE"
        success "Namespace created."
    fi

    # Create ServiceAccount
    info "Creating ServiceAccount '$SA_NAME'..."
    oc apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: network-health-poc
EOF
    success "ServiceAccount created."

    # Bind the ServiceAccount to hostnetwork-v2 SCC
    info "Granting hostnetwork-v2 SCC to ServiceAccount..."
    oc adm policy add-scc-to-user hostnetwork-v2 \
        "system:serviceaccount:${NAMESPACE}:${SA_NAME}" 2>/dev/null || \
    oc adm policy add-scc-to-user hostnetwork \
        "system:serviceaccount:${NAMESPACE}:${SA_NAME}" 2>/dev/null || \
    warn "Could not bind hostnetwork SCC â€” listener pods may fail to schedule."
    success "SCC binding applied."

    # Deploy DaemonSet
    info "Deploying TCP listener DaemonSet on all nodes (port ${LISTENER_PORT})..."
    oc apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ${DAEMONSET_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: network-health-poc
    component: tcp-listener
spec:
  selector:
    matchLabels:
      app: network-health-poc
      component: tcp-listener
  template:
    metadata:
      labels:
        app: network-health-poc
        component: tcp-listener
    spec:
      serviceAccountName: ${SA_NAME}
      hostNetwork: true
      tolerations:
        - operator: Exists
      containers:
        - name: listener
          image: ${BUSYBOX_IMAGE}
          command:
            - /bin/sh
            - -c
            - |
              echo "Starting TCP listener on port ${LISTENER_PORT}..."
              while true; do
                echo "HEALTH_OK" | nc -l -p ${LISTENER_PORT} 2>/dev/null || true
              done
          ports:
            - containerPort: ${LISTENER_PORT}
              hostPort: ${LISTENER_PORT}
              protocol: TCP
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 50m
              memory: 32Mi
EOF
    success "DaemonSet deployed."

    info "Waiting for listener pods to become ready..."
    if oc rollout status daemonset/"${DAEMONSET_NAME}" -n "$NAMESPACE" --timeout=60s 2>/dev/null; then
        success "All listener pods are running."
    else
        warn "Some listener pods may not be ready yet. Check with: $0 status"
    fi

    echo ""
    success "Listener setup complete. Each node now has a TCP listener on port ${LISTENER_PORT}."
    info "Next step: run '$0 setup-handshake-monitor' to start monitoring."
}

###############################################################################
# setup-handshake-monitor: Deploy TCP handshake monitor pod
###############################################################################
cmd_setup_handshake_monitor() {
    check_oc

    if ! namespace_exists; then
        error "Namespace '$NAMESPACE' does not exist. Run '$0 setup-listeners' first."
        exit 1
    fi

    # Verify listeners are running
    local listener_count
    listener_count=$(oc get pods -n "$NAMESPACE" -l component=tcp-listener --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    if [[ "$listener_count" -eq 0 ]]; then
        error "No listener pods are running. Run '$0 setup-listeners' first."
        exit 1
    fi
    info "Found ${listener_count} running listener pod(s)."

    setup_monitor_access

    info "Deploying TCP handshake monitor pod..."
    oc apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${HANDSHAKE_MONITOR_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: network-health-poc
    component: handshake-monitor
spec:
  serviceAccountName: ${MONITOR_SA_NAME}
  containers:
    - name: monitor
      image: ${BUSYBOX_IMAGE}
      command:
        - /bin/sh
        - -c
        - |
          echo "=== TCP Handshake Monitor Starting ==="
          echo "Monitoring port ${LISTENER_PORT} on all nodes"
          echo "Check interval: 5 seconds"
          echo "======================================="
$(node_discovery_script)
          while true; do
            TIMESTAMP=\$(date '+%Y-%m-%d %H:%M:%S')
            NODE_IPS=\$(discover_node_ips)

            if [ -z "\$NODE_IPS" ]; then
              echo "[\$TIMESTAMP] WARN: Could not discover node IPs, retrying..."
              sleep 5
              continue
            fi

            for NODE_IP in \$NODE_IPS; do
              if nc -z -w 2 "\$NODE_IP" ${LISTENER_PORT} 2>/dev/null; then
                echo "[\$TIMESTAMP] Node \$NODE_IP: HEALTHY (TCP handshake on port ${LISTENER_PORT} succeeded)"
              else
                echo "[\$TIMESTAMP] Node \$NODE_IP: UNHEALTHY (TCP handshake on port ${LISTENER_PORT} failed)"
              fi
            done
            echo "[\$TIMESTAMP] --- check cycle complete ---"
            sleep 5
          done
      resources:
        requests:
          cpu: 10m
          memory: 16Mi
        limits:
          cpu: 50m
          memory: 32Mi
  restartPolicy: Always
EOF
    success "Handshake monitor pod deployed."

    info "Waiting for monitor pod to start..."
    oc wait pod/"${HANDSHAKE_MONITOR_NAME}" -n "$NAMESPACE" \
        --for=condition=Ready --timeout=60s 2>/dev/null || \
        warn "Monitor pod may not be ready yet."

    echo ""
    success "Handshake monitor is running."
    info "View logs: $0 logs handshake"
}

###############################################################################
# setup-service-monitor: Deploy kubelet service monitor pod
###############################################################################
cmd_setup_service_monitor() {
    check_oc

    if ! namespace_exists; then
        error "Namespace '$NAMESPACE' does not exist. Run '$0 setup-listeners' first to create it,"
        error "or create it manually: oc new-project $NAMESPACE"
        exit 1
    fi

    setup_monitor_access

    info "Deploying kubelet service monitor pod (port 10250)..."
    oc apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${SERVICE_MONITOR_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: network-health-poc
    component: service-monitor
spec:
  serviceAccountName: ${MONITOR_SA_NAME}
  containers:
    - name: monitor
      image: ${BUSYBOX_IMAGE}
      command:
        - /bin/sh
        - -c
        - |
          echo "=== Kubelet Service Monitor Starting ==="
          echo "Monitoring kubelet on port 10250 on all nodes"
          echo "Check interval: 5 seconds"
          echo "========================================="
$(node_discovery_script)
          while true; do
            TIMESTAMP=\$(date '+%Y-%m-%d %H:%M:%S')
            NODE_IPS=\$(discover_node_ips)

            if [ -z "\$NODE_IPS" ]; then
              echo "[\$TIMESTAMP] WARN: Could not discover node IPs, retrying..."
              sleep 5
              continue
            fi

            for NODE_IP in \$NODE_IPS; do
              if nc -z -w 2 "\$NODE_IP" 10250 2>/dev/null; then
                echo "[\$TIMESTAMP] Node \$NODE_IP kubelet: HEALTHY (TCP connect to port 10250 succeeded)"
              else
                echo "[\$TIMESTAMP] Node \$NODE_IP kubelet: UNHEALTHY (TCP connect to port 10250 failed)"
              fi
            done
            echo "[\$TIMESTAMP] --- check cycle complete ---"
            sleep 5
          done
      resources:
        requests:
          cpu: 10m
          memory: 16Mi
        limits:
          cpu: 50m
          memory: 32Mi
  restartPolicy: Always
EOF
    success "Service monitor pod deployed."

    info "Waiting for monitor pod to start..."
    oc wait pod/"${SERVICE_MONITOR_NAME}" -n "$NAMESPACE" \
        --for=condition=Ready --timeout=60s 2>/dev/null || \
        warn "Monitor pod may not be ready yet."

    echo ""
    success "Service monitor is running."
    info "View logs: $0 logs service"
}

###############################################################################
# teardown: Remove all POC resources
###############################################################################
cmd_teardown() {
    check_oc

    info "Tearing down Network Health POC resources..."

    # Remove SCC bindings
    info "Removing SCC bindings..."
    oc adm policy remove-scc-from-user hostnetwork-v2 \
        "system:serviceaccount:${NAMESPACE}:${SA_NAME}" 2>/dev/null || true
    oc adm policy remove-scc-from-user hostnetwork \
        "system:serviceaccount:${NAMESPACE}:${SA_NAME}" 2>/dev/null || true
    success "SCC bindings removed."

    info "Removing monitor node discovery permissions..."
    oc delete clusterrolebinding "$MONITOR_ROLE_NAME" --ignore-not-found
    oc delete clusterrole "$MONITOR_ROLE_NAME" --ignore-not-found

    # Delete namespace (cascades to all resources within it)
    if namespace_exists; then
        info "Deleting namespace '$NAMESPACE' (this removes all pods, DaemonSets, ServiceAccounts)..."
        oc delete namespace "$NAMESPACE" --wait=false
        success "Namespace deletion initiated."
        info "Namespace may take a moment to fully terminate."
    else
        info "Namespace '$NAMESPACE' does not exist â€” nothing to delete."
    fi

    echo ""
    success "Teardown complete."
}

###############################################################################
# status: Show state of all deployed resources
###############################################################################
cmd_status() {
    check_oc

    echo -e "\n${BOLD}=== Network Health POC Status ===${NC}\n"

    if ! namespace_exists; then
        warn "Namespace '$NAMESPACE' does not exist. POC is not deployed."
        return
    fi

    success "Namespace '$NAMESPACE' exists."
    echo ""

    # Listener DaemonSet
    echo -e "${BOLD}--- TCP Listener DaemonSet ---${NC}"
    if oc get daemonset "$DAEMONSET_NAME" -n "$NAMESPACE" &>/dev/null; then
        oc get daemonset "$DAEMONSET_NAME" -n "$NAMESPACE" -o wide 2>/dev/null
        echo ""
        echo -e "${BOLD}Listener Pods:${NC}"
        oc get pods -n "$NAMESPACE" -l component=tcp-listener -o wide 2>/dev/null
    else
        warn "Listener DaemonSet not deployed."
    fi
    echo ""

    # Handshake Monitor
    echo -e "${BOLD}--- Handshake Monitor ---${NC}"
    if oc get pod "$HANDSHAKE_MONITOR_NAME" -n "$NAMESPACE" &>/dev/null; then
        local phase
        phase=$(oc get pod "$HANDSHAKE_MONITOR_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)
        if [[ "$phase" == "Running" ]]; then
            echo -e "Status: ${GREEN}Running${NC}"
        else
            echo -e "Status: ${RED}${phase}${NC}"
        fi
        oc get pod "$HANDSHAKE_MONITOR_NAME" -n "$NAMESPACE" -o wide 2>/dev/null
    else
        warn "Handshake monitor not deployed."
    fi
    echo ""

    # Service Monitor
    echo -e "${BOLD}--- Service Monitor ---${NC}"
    if oc get pod "$SERVICE_MONITOR_NAME" -n "$NAMESPACE" &>/dev/null; then
        local phase
        phase=$(oc get pod "$SERVICE_MONITOR_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)
        if [[ "$phase" == "Running" ]]; then
            echo -e "Status: ${GREEN}Running${NC}"
        else
            echo -e "Status: ${RED}${phase}${NC}"
        fi
        oc get pod "$SERVICE_MONITOR_NAME" -n "$NAMESPACE" -o wide 2>/dev/null
    else
        warn "Service monitor not deployed."
    fi
    echo ""

    # Nodes
    echo -e "${BOLD}--- Nodes ---${NC}"
    oc get nodes -o wide 2>/dev/null || \
        warn "Could not list nodes."
    echo ""
}

###############################################################################
# logs: Tail monitor pod logs
###############################################################################
cmd_logs() {
    local monitor_type="${1:-}"

    check_oc

    if ! namespace_exists; then
        error "Namespace '$NAMESPACE' does not exist. Deploy monitors first."
        exit 1
    fi

    case "$monitor_type" in
        handshake)
            info "Tailing handshake monitor logs (Ctrl+C to stop)..."
            echo ""
            oc logs -f "$HANDSHAKE_MONITOR_NAME" -n "$NAMESPACE"
            ;;
        service)
            info "Tailing service monitor logs (Ctrl+C to stop)..."
            echo ""
            oc logs -f "$SERVICE_MONITOR_NAME" -n "$NAMESPACE"
            ;;
        *)
            error "Usage: $0 logs <handshake|service>"
            exit 1
            ;;
    esac
}

###############################################################################
# Usage
###############################################################################
usage() {
    cat <<USAGE
${BOLD}Network-Based Node Health Detection POC${NC}
Inspired by RHWA-450

${BOLD}Usage:${NC}
  $0 <command>

${BOLD}Commands:${NC}
  setup-listeners          Deploy TCP listener DaemonSet on all nodes
  setup-handshake-monitor  Deploy TCP handshake monitor pod (requires listeners)
  setup-service-monitor    Deploy kubelet service monitor pod (checks port 10250)
  teardown                 Remove all POC resources
  status                   Show the state of all deployed resources
  logs <handshake|service> Tail the logs of a monitor pod

${BOLD}Quick Start:${NC}
  $0 setup-listeners
  $0 setup-handshake-monitor
  $0 logs handshake

${BOLD}Alternative (no listeners needed):${NC}
  $0 setup-listeners       # Creates namespace and SA
  $0 setup-service-monitor
  $0 logs service
USAGE
}

###############################################################################
# Main
###############################################################################
main() {
    local command="${1:-}"
    shift || true

    case "$command" in
        setup-listeners)
            cmd_setup_listeners
            ;;
        setup-handshake-monitor)
            cmd_setup_handshake_monitor
            ;;
        setup-service-monitor)
            cmd_setup_service_monitor
            ;;
        teardown)
            cmd_teardown
            ;;
        status)
            cmd_status
            ;;
        logs)
            cmd_logs "$@"
            ;;
        -h|--help|help)
            usage
            ;;
        "")
            usage
            exit 1
            ;;
        *)
            error "Unknown command: $command"
            echo ""
            usage
            exit 1
            ;;
    esac
}

main "$@"
