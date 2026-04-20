#!/usr/bin/env bash
#
# Usage: {script} {machine datapath DNS name}
#        ./deploySimulator sm4u-13
#
# Deploys the tape backend simulator to the machine specified.
# The machine specified must have scp installed on it.

host=$1
case "$host" in
	''|'-h')
		echo "Usage: $0 <host>"
		exit 1
		;;
esac

ipaddr=$(echo $1 | awk '$1 ~ /^([0-9]{1,3}\.){1,3}[0-9]+$/ {print $1}')

if [ -z "$ipaddr" ]; then
	case "$host" in
		*.eng.sldomain.com)
			;;
		*-mgmt)
			host="${host}.eng.sldomain.com"
			;;
		*)
			host="${host}-mgmt.eng.sldomain.com"
			;;
	esac
fi

SSH_BASE="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="$SSH_BASE -l root $host"
SCP="scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r"

echo "Stopping services..."
set -e
$SSH "monit stop tomcat"
$SSH "monit stop dataplanner"
$SSH "monit stop tape_backend_sim"
$SSH "service tomcat9 stop" || true
$SSH "service dataplanner stop" || true
$SSH "service tape_backend_sim stop" || true
echo "Deploying software..."

destination_simulator="/usr/local/bluestorm/frontend/simulator"

cwdf="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cwdd="/cygdrive/c/"
cwd=${cwdf/C:\\/$cwdd}
echo "Current working directory: $cwd"

$SSH "zfs set readonly=off system/root0"

deploy_dir="/deploy_temp/"
full_deploy_dir=$cwd$deploy_dir
rm -rf $full_deploy_dir
mkdir $full_deploy_dir

tarfile="simulator-0.0.4-SNAPSHOT.tar"
built_service="/simulator/build/distributions/$tarfile"
full_built_service=$cwd$built_service
tar -xvf $full_built_service -C $full_deploy_dir --strip-components=1

echo "scp -r $full_deploy_dir to root@$host:$destination_simulator"
$SSH "rm -rf $destination_simulator"
$SCP -r $full_deploy_dir root@$host:$destination_simulator

echo "Starting services..."
$SSH "monit start tape_backend_sim"
$SSH "monit start dataplanner"
$SSH "monit start tomcat"


echo "Cleaning up..."
rm -rf $full_deploy_dir

echo "Done."
