#!/bin/bash

# -----------------------------------------------------------------------------
# This script checks if Azurite and  are running.
# Those containers are required to run unit tests for Azure and AWS.
# If they are already running, it does nothing.
# If either or both containers are not running, it starts them using docker-compose.
# -----------------------------------------------------------------------------

# Define the container names
Azurite="azurite"
Localstack="localstack"

# Define the docker-compose file
Azurite_File="docker/docker-compose-azurite.yml"
Localstack_File="docker/docker-compose-localstack.yml"

# Function to check if a container is running
is_container_running() {
  container_name="$1"  # Take container name as an argument
  docker ps --filter "name=${container_name}" --format '{{.Names}}' | grep -w "${container_name}"
}

# Maximum wait time (in seconds)
TIMEOUT=60
SLEEP_INTERVAL=5  # Check interval in seconds

# Check if both containers are running
Azurite_Running=$(is_container_running "${Azurite}")
Localstack_Running=$(is_container_running "${Localstack}")

All_Running=false
if [[ -n "$Azurite_Running" && -n "$Localstack_Running" ]]; then
  All_Running=true
  echo "Both containers '$Azurite' and '$Localstack' are already running."
else
    echo "Starting missing containers"
    if [[ -z "$Azurite_Running" ]]; then
      echo "Starting '$Azurite'..."
      docker-compose -f $Azurite_File up -d
    fi

    if [[ -z "$Localstack_Running" ]]; then
      echo "Starting '$Localstack'..."
      docker-compose -f $Localstack_File up -d
    fi
    # Wait for all containers to be ready
    echo "Waiting for all containers to be fully running..."
    SECONDS=0
    while [ $SECONDS -lt $TIMEOUT ]; do
        ALL_READY=true
        if ! is_container_running "$Azurite"; then
            echo "Waiting for $Azurite to be ready..."
            ALL_READY=false
        fi

        if ! is_container_running "$Localstack"; then
            echo "Waiting for Localstack to be ready..."
            ALL_READY=false
        fi

        if [ "$ALL_READY" = true ]; then
            echo "All containers are now running!"
            break
        fi

        sleep $SLEEP_INTERVAL
    done

fi
