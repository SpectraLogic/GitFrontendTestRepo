#!/bin/bash

# Run testAll.sh with most integration tests skipped for faster execution
# SKIP_RPC_TESTS=true
SKIP_DATAPLANNER_TESTS=true SKIP_DOCKER_INTEGRATION=true SKIP_PUBLIC_CLOUD_TESTS=true ./testAll.sh