#!/bin/bash

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is NOT installed. Exiting."
  exit 1
else
  if ! docker info &> /dev/null; then
      echo "Docker is not running.Exiting"
      exit 1
  fi
fi

# Function to check if a container is running
is_container_running() {
  container_name="$1"  # Take container name as an argument
  docker ps --filter "name=${container_name}" --format '{{.Names}}' | grep -w "${container_name}"
}

all_containers_running() {
  # Check if all containers are running
  dnsmasqRunning=$(is_container_running "dnsmasq")
  azuriteRunning=$(is_container_running "azurite")
  localstackRunning=$(is_container_running "localstack")
  postgresRunning=$(is_container_running "postgres")
  postgresReplicaRunning=$(is_container_running "postgresreplica")
  tomcatRunning=$(is_container_running "tomcat")
  tomcatRunning=$(is_container_running "tomcatreplica")
  dataplannerRunning=$(is_container_running "dataplanner")
  dataplannerReolicaRunning=$(is_container_running "dataplannerreplica")
  simulatorRunning=$(is_container_running "simulator")

  if [ -n "$dnsmasqRunning" ] && [ -n "$azuriteRunning" ] && [ -n "$localstackRunning" ] && [ -n "$postgresRunning" ] && [ -n "$tomcatRunning" ] && [ -n "$dataplannerRunning" ] && [ -n "$simulatorRunning" ]; then
      return 0
  else
      echo "At least one service is not running."
      return 1
  fi
}

./packageAll.sh




# Function to setup docker environment
cleanup_docker_env() {
  echo "Cleaning up Docker environment..."

    # 1. Get a list of all container IDs
    containers=$(docker ps -aq)
    if [ -n "$containers" ]; then
      echo "Stopping and removing containers..."
      docker stop $containers > /dev/null 2>&1
      docker rm -f $containers > /dev/null 2>&1
    fi

    # 2. Prune everything (Images, Networks, and Build Cache)
    # -a removes all unused images, not just dangling ones
    docker system prune -a -f > /dev/null 2>&1
    docker network prune -f > /dev/null 2>&1

    # 3. Handle Volumes
    # We do this last and use 'prune' instead of 'rm' to avoid "in use" errors
    # Or use 'docker volume rm' with a check
    volumes=$(docker volume ls -q)
    if [ -n "$volumes" ]; then
      echo "Removing volumes..."
      docker volume rm $volumes > /dev/null 2>&1
    fi

    # 4. Final safety check for any remaining anonymous volumes
    docker volume prune -f > /dev/null 2>&1
}

cleanup_docker_env

echo "Starting docker containers for the test..."
COMPOSE_FILE="docker/docker-compose-replica.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
  echo "Error: docker-compose file '$COMPOSE_FILE' not found."
  exit 1
fi
docker-compose -f $COMPOSE_FILE up -d


TIMEOUT=360
SLEEP_INTERVAL=5  # Check interval in seconds


if ! all_containers_running; then
   # Wait for all containers to be ready
   SECONDS=0
   while [ $SECONDS -lt $TIMEOUT ]; do
     if all_containers_running; then
       echo "All containers are now running!"
       break
     fi
     sleep $SLEEP_INTERVAL
   done
fi

if ! all_containers_running; then
  echo "All containers are not running.Exiting"
  exit 1
fi

# It takes a little time to create default data policies.
sleep 200
echo "Running integration tests..."
DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"

[ -n "$BUILD_DEBUG" ] && OPTS="--stacktrace --debug"


export LANG="en_US.UTF-8"

#$GRADLE $OPTS  clean integrationtests:test -PincludeTags="LocalDevelopment" --max-workers=1

$GRADLE $OPTS  clean integrationtests:test  --max-workers=1

cleanup_docker_env