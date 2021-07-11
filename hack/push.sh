#! /bin/bash

make bin/unpacker
podman build -f Dockerfile.unpacker -t quay.io/tflannag/manifest:unpacker
podman push quay.io/tflannag/manifest:unpacker
