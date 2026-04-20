#!/usr/bin/env bash
#
# Usage: {script} {machine datapath DNS name}
#        ./deployFrontEndServerAndDataPlanner sm4u-13
#
# Deploys the front end S3 server and data planner to the machine specified.
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
$SSH "service tomcat9 stop" || true
$SSH "service dataplanner stop" || true
echo "Deploying software..."

destination_webapps="/var/run/tomcat/webapps"
destination_planner="/usr/local/bluestorm/frontend/dataplanner"

cwdf="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cwdd="/cygdrive/c/"
cwd=${cwdf/C:\\/$cwdd}
echo "Current working directory: $cwd"

$SSH "zfs set readonly=off system/root0"

deploy_dir="/deploy_temp/"
full_deploy_dir=$cwd$deploy_dir
rm -rf $full_deploy_dir
mkdir $full_deploy_dir

dest="ROOT.war"
full_dest=$full_deploy_dir$dest
built_service="/server/build/libs/server-0.0.4-SNAPSHOT.war"
full_built_service=$cwd$built_service
cp $full_built_service $full_dest

echo "scp $full_deploy_dir to root@$host:$destination_webapps"
$SSH "rm -rf $destination_webapps"
$SCP $full_deploy_dir root@$host:$destination_webapps

$SSH "chmod 777 $destination_webapps/* $destination_webapps"

rm -rf $full_deploy_dir
mkdir $full_deploy_dir

tarfile="DataPlanner-0.0.4-SNAPSHOT.tar"
built_service="/DataPlanner/build/distributions/$tarfile"
full_built_service=$cwd$built_service
tar -xvf $full_built_service -C $full_deploy_dir --strip-components=1

echo "scp -r $full_deploy_dir to root@$host:$destination_planner"
$SSH "rm -rf $destination_planner"
$SCP -r $full_deploy_dir root@$host:$destination_planner

echo "Starting services..."
$SSH "monit start tomcat"
$SSH "monit start dataplanner"

echo "Cleaning up..."
rm -rf $full_deploy_dir

echo "Done."
