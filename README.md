# rukpak

Rukpak runs in a Kubernetes cluster and defines an API for installing cloud native bundle content

## Installation

Run the following make target to install the rukpak provisioning stack locally:

```bash
make install
```

The component responsible for unpacking bundle content requires root permissions in order to create filesystem content.

When running on OpenShift clusters, ensure the `admin` serviceaccount resource has the `anyuid` SSC permissions:

```bash
oc -n rukpak adm policy add-scc-to-user anyuid -z admin
```
