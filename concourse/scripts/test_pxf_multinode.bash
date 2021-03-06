#!/bin/bash

set -exuo pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"

SSH_OPTS=(-i cluster_env_files/private_key.pem -o 'StrictHostKeyChecking=no')
HADOOP_SSH_OPTS=(-o 'StrictHostKeyChecking=no')

LOCAL_GPHD_ROOT=/singlecluster

function configure_local_hdfs() {
	sed -i -e "s|hdfs://0.0.0.0:8020|hdfs://${HADOOP_HOSTNAME}:8020|" \
	${LOCAL_GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml ${LOCAL_GPHD_ROOT}/hbase/conf/hbase-site.xml
}

function run_multinode_smoke_test() {
	NO_OF_FILES=${1:-300}
	echo "Running multinode smoke test with ${NO_OF_FILES} files"
	time ssh "${HADOOP_SSH_OPTS[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAME}" "
		export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
		command -v kinit && kinit gpadmin -kt pxf.service.keytab
		${HDFS_BIN}/hdfs dfs -mkdir -p /tmp && mkdir -p /tmp/pxf_test &&
		for ((i=1; i<=${NO_OF_FILES}; i++)); do
			cat > /tmp/pxf_test/test_\${i}.txt <<-EOF
				1
				2
				3
			EOF
		done &&
		${HDFS_BIN}/hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp
	"

	echo "Found $("${LOCAL_GPHD_ROOT}/bin/hdfs" dfs -ls /tmp/pxf_test | grep -c pxf_test) items in /tmp/pxf_test"
	expected_output=$((3 * NO_OF_FILES))

	time ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		source ${GPHOME}/greenplum_path.sh
		psql -d template1 -c \"
			CREATE EXTERNAL TABLE pxf_multifile_test (b TEXT)
				LOCATION ('pxf://tmp/pxf_test?PROFILE=HdfsTextSimple')
				FORMAT 'CSV';
		\"
		num_rows=\$(psql -d template1 -t -c 'SELECT COUNT(*) FROM pxf_multifile_test;' | head -1)
		if (( num_rows == ${expected_output} )); then
			echo 'Received expected output'
		else
			echo \"Error. Expected output '${expected_output}' does not match actual '\${num_rows}'\"
			exit 1
		fi
	"
}

function update_pghba_conf() {
	local sdw_ips=("$@")
	for ip in "${sdw_ips[@]}"; do
		echo "host	all	gpadmin		$ip/32	trust"
	done | ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		cat >> /data/gpdata/master/gpseg-1/pg_hba.conf &&
		cat /data/gpdata/master/gpseg-1/pg_hba.conf
	"
}

function setup_pxf_on_cluster() {
	# drop named query file for JDBC test to gpadmin's home on mdw
	scp "${SSH_OPTS[@]}" pxf_src/automation/src/test/resources/{,hive-}report.sql gpadmin@mdw:

	# init all PXFs using cluster command, configure PXF on master, sync configs and start pxf
	ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		source ${GPHOME}/greenplum_path.sh &&
		if [[ ! -d ${PXF_CONF_DIR} ]]; then
			PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init
			cp ${PXF_CONF_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ${PXF_CONF_DIR}/servers/default
			sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g' ${PXF_CONF_DIR}/servers/default/*-site.xml
		else
			cp ${PXF_CONF_DIR}/templates/mapred{,1}-site.xml
		fi &&
		mkdir -p ${PXF_CONF_DIR}/servers/s3{,-invalid} &&
		cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3 &&
		cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3-invalid &&
		sed -i  -e \"s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|\" \
			-e \"s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|\" \
			${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/database &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/database/ &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://mdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER|gpadmin|' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${PXF_CONF_DIR}/servers/database/jdbc-site.xml &&
		cp ~gpadmin/report.sql ${PXF_CONF_DIR}/servers/database &&
		cp ${PXF_CONF_DIR}/servers/database/jdbc-site.xml ${PXF_CONF_DIR}/servers/database/testuser-user.xml &&
		sed -i 's|pxfautomation|template1|' ${PXF_CONF_DIR}/servers/database/testuser-user.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/db-session-params &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-session-params &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://mdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			-e 's|</configuration>|<property><name>jdbc.session.property.client_min_messages</name><value>debug1</value></property></configuration>|' \
			-e 's|</configuration>|<property><name>jdbc.session.property.default_statistics_target</name><value>123</value></property></configuration>|' \
			${PXF_CONF_DIR}/servers/db-session-params/jdbc-site.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/db-hive &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-hive &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
			-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HADOOP_HOSTNAME}:10000/default|\" \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${PXF_CONF_DIR}/servers/db-hive/jdbc-site.xml &&
		cp ~gpadmin/hive-report.sql ${PXF_CONF_DIR}/servers/db-hive &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"
}

function run_pxf_automation() {
	local multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiNodesCluster.xml
	if (( HIVE_VERSION == 2 )); then
		local search='<hiveBaseHdfsDirectory>/hive/warehouse/</hiveBaseHdfsDirectory>'
		local replace='<hiveBaseHdfsDirectory>/user/hive/warehouse/</hiveBaseHdfsDirectory>'
		sed -i "s|${search}|${replace}|g" "$multiNodesCluster"
	fi

	# point the tests at remote Hadoop and GPDB
	sed -i "s/>hadoop</>${HADOOP_HOSTNAME}</g" "$multiNodesCluster"
	sed -i "/<class>org.greenplum.pxf.automation.components.gpdb.Gpdb<\/class>/ {n; s/localhost/mdw/}" \
		"$multiNodesCluster"

	if [[ $KERBEROS == true ]]; then
		DATAPROC_DIR=$(find /tmp/build/ -name dataproc_env_files)
		REALM=$(< "${DATAPROC_DIR}/REALM")
		REALM=${REALM^^} # make sure REALM is up-cased, down-case below for hive principal
		KERBERIZED_HADOOP_URI="hive/${HADOOP_HOSTNAME}.${REALM,,}@${REALM};saslQop=auth-conf" # quoted because of semicolon
		sed -i  -e "s|</hdfs>|<hadoopRoot>$DATAPROC_DIR</hadoopRoot></hdfs>|g" \
			-e "s|</cluster>|<testKerberosPrincipal>gpadmin@${REALM}</testKerberosPrincipal></cluster>|g" \
			-e "s|</hive>|<kerberosPrincipal>${KERBERIZED_HADOOP_URI}</kerberosPrincipal><userName>gpadmin</userName></hive>|g" \
			"$multiNodesCluster"
		ssh gpadmin@mdw "
			sed -i -e 's|\(jdbc:hive2://${HADOOP_HOSTNAME}:10000/default\)|\1;principal=${KERBERIZED_HADOOP_URI}|g' \
				${PXF_CONF_DIR}/servers/db-hive/jdbc-site.xml &&
			${GPHOME}/pxf/bin/pxf cluster sync
		"
		sudo mkdir -p /etc/security/keytabs
		sudo cp "${DATAPROC_DIR}/pxf.service.keytab" /etc/security/keytabs/gpadmin.headless.keytab
		sudo chown gpadmin:gpadmin /etc/security/keytabs/gpadmin.headless.keytab
		sudo cp "${DATAPROC_DIR}/krb5.conf" /etc/krb5.conf
	fi

	sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/automation/jsystem.properties
	chown -R gpadmin:gpadmin ~gpadmin/{.ssh,pxf} pxf_src/automation

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		set -exuo pipefail

		source ${GPHOME}/greenplum_path.sh

		export PXF_HOME=\${GPHOME}/pxf
		export PGHOST=mdw
		export PGPORT=5432

		export ACCESS_KEY_ID='${ACCESS_KEY_ID}' SECRET_ACCESS_KEY='${SECRET_ACCESS_KEY}'

		make -C ${PWD}/pxf_src/automation GROUP=${GROUP}
	EOF

	chown gpadmin:gpadmin ~gpadmin/run_pxf_automation_test.sh
	chmod a+x ~gpadmin/run_pxf_automation_test.sh

	if [[ ${ACCEPTANCE} == true ]]; then
		echo 'Acceptance test pipeline'
		exit 1
	fi

	# needs to be login shell for UTF-8 tests (LOCALE)
	su --login gpadmin -c ~gpadmin/run_pxf_automation_test.sh
}

function _main() {
	cp -R cluster_env_files/.ssh/* /root/.ssh
	# make an array, gpdb_segments, containing hostnames that contain 'sdw'
	mapfile -t gpdb_segments < <(grep < cluster_env_files/etc_hostfile -e sdw | awk '{print $1}')
	if [[ -d dataproc_env_files ]]; then
		HADOOP_HOSTNAME=$(< dataproc_env_files/name)
		HADOOP_USER=gpadmin
		hadoop_ip=$(getent hosts "$HADOOP_HOSTNAME" | awk '{ print $1 }')
		HADOOP_SSH_OPTS+=(-i dataproc_env_files/google_compute_engine)
		HDFS_BIN=/usr/bin
	else
		HADOOP_HOSTNAME=hadoop
		HADOOP_USER=centos
		HDFS_BIN=~centos/singlecluster/bin
		hadoop_ip=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
		# tell hbase where to find zookeeper
		sed -i "/<name>hbase.zookeeper.quorum<\/name>/ {n; s/127.0.0.1/${hadoop_ip}/}" \
			"${LOCAL_GPHD_ROOT}/hbase/conf/hbase-site.xml"
	fi

	install_gpdb_binary # Installs the GPDB Binary on the container
	setup_gpadmin_user
	install_pxf_server
	init_and_configure_pxf_server
	remote_access_to_gpdb

	configure_local_hdfs

	# widen access to mdw to all nodes in the cluster for JDBC test
	update_pghba_conf "${gpdb_segments[@]}"

	setup_pxf_on_cluster

	run_multinode_smoke_test 1000
	run_pxf_automation
}

_main
