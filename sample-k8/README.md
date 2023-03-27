## Prerequisites

1. Local Minikube cluster
2. kubectl command line utility
3. Mount your home directory on a /data folder for minikube

```bash
minikube mount $HOME:/data
```

## Running


1. Run the k8-setup.sh to do an instant setup.
```
./k8-setup.sh
```
2. k8-cleanup.sh script can be run to do a complete cleanup.