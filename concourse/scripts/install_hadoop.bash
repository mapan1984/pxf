#!/bin/bash

set -exuo pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"

SSH_OPTS=(-i cluster_env_files/private_key.pem)
LOCAL_GPHD_ROOT=/singlecluster
REMOTE_GPHD_ROOT=~centos/singlecluster

function install_hadoop_single_cluster() {
	local hadoop_ip=${1}

	tar -xzf pxf_tarball/pxf.tar.gz -C /tmp
	cp /tmp/pxf/lib/pxf-hbase-*.jar "${LOCAL_GPHD_ROOT}/hbase/lib"

	cat <<-EOF > ~/setup_hadoop.sh
		yum install -y -d 1 java-1.8.0-openjdk-devel &&
		export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk &&

		export PATH=\$PATH:${REMOTE_GPHD_ROOT}/bin:${REMOTE_GPHD_ROOT}/hbase/bin:${REMOTE_GPHD_ROOT}/hive/bin:${REMOTE_GPHD_ROOT}/zookeeper/bin &&

		sed -i 's/edw0/hadoop/' /etc/hosts &&
		sed -i -e 's/>tez/>mr/g' -e 's/localhost/${hadoop_ip}/g' ${REMOTE_GPHD_ROOT}/hive/conf/hive-site.xml &&
		sed -i -e 's/0.0.0.0/${hadoop_ip}/g' ${REMOTE_GPHD_ROOT}/hadoop/etc/hadoop/{core,hdfs,yarn}-site.xml &&

		groupadd supergroup &&
		usermod -aG supergroup gpadmin &&

		source ~centos/pxf_common.bash &&
		export IMPERSONATION=${IMPERSONATION} &&
		setup_impersonation ${REMOTE_GPHD_ROOT} &&
		start_hadoop_services ${REMOTE_GPHD_ROOT}
	EOF

	scp "${SSH_OPTS[@]}" -rq \
		~/setup_hadoop.sh \
		pxf_src/concourse/scripts/pxf_common.bash \
		"${LOCAL_GPHD_ROOT}" \
		centos@edw0:

	ssh "${SSH_OPTS[@]}" centos@edw0 '
		sudo chmod +x ~/setup_hadoop.sh &&
		sudo ~/setup_hadoop.sh
	'
}

function _main() {
	cp -R cluster_env_files/.ssh/* /root/.ssh

	hadoop_ip=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
	install_hadoop_single_cluster "$hadoop_ip"
}

_main
