# node-network-check-operator
// TODO(user): Add simple overview of use/purpose

## Description
// TODO(user): An in-depth paragraph about your project and overview of use

## Getting Started

### Prerequisites
- Go 1.26.0 or newer (the preferred toolchain is Go 1.26.5).
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

With `GOTOOLCHAIN=auto`, Go uses a newer installed toolchain or downloads the preferred version when needed.

### Deploy with the medik8s development tools

The shared [medik8s development tools](https://github.com/medik8s/tools/tree/main/dev) provide the same `dev-*` targets used by the other operators. To use an up-to-date existing checkout, set `TOOLS_DIR` to its path:

```sh
export TOOLS_DIR=/path/to/medik8s/tools
make dev-help
```

If `TOOLS_DIR` is not set, a sibling `../tools` checkout is used when available; otherwise the tools are downloaded to `.tools/` on the first `dev-*` command. For `dev-*` targets, Podman is the default container tool; set `CONTAINER_TOOL=docker` to use Docker instead.

For a local Kind cluster:

```sh
make dev-setup
make dev-deploy
make dev-logs
```

For an existing cluster:

```sh
export KUBECONFIG=/path/to/kubeconfig
export SKIP_KIND=true
make dev-setup
make dev-deploy
make dev-logs
```

`dev-setup` configures the selected cluster; `dev-deploy` pushes the development image to `ttl.sh` by default for external clusters.

For a development OLM installation on an existing cluster with OLM installed:

```sh
export KUBECONFIG=/path/to/kubeconfig
export SKIP_KIND=true
make dev-olm-deploy DEV_REGISTRY=ttl.sh \
  DEV_OLM_OPERATOR_NAMESPACE=node-network-check-operator-system
```

The shared OLM target cleans up an existing installation of this package with `--delete-all` before installing. This can remove its CRDs and custom resources. The `ttl.sh` images expire after two hours by default.

### Service network checks

Create a `NodeNetworkCheckOperatorConfig` to start checks. In `Service` or `Both`
mode, the operator attempts a TCP connection to the kubelet on port 10250 for
each node. The sample configuration uses a five-second interval and marks a node
unhealthy after three consecutive failures. Both settings are configurable.

The operator sets the node condition `NNCOServiceUnhealthy=True` after the
failure threshold and removes it when the check succeeds. It also removes its
condition when service checks are disabled or the last configuration is deleted.
Nodes without a usable IP address are skipped. If multiple configurations exist,
the oldest is active. Failure counts reset when the operator restarts. TCP
listener checks are not implemented yet.

Checks run from the operator pod, so a network problem affecting that pod can
make healthy nodes appear unreachable.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/node-network-check-operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified. 
And it is required to have access to pull the image from the working environment. 
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/node-network-check-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin 
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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

