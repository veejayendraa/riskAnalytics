#!/bin/bash


set -x

THIS_SCRIPT=$0

start_ignite_node()
{
	mkdir /home/hadoop/ignite
	
	echo "ignite directory created"
	
	aws s3 cp s3://ignite-binary/ignite1.6.zip /home/hadoop/ignite
	cd /home/hadoop/ignite
	unzip ignite1.6.zip
	
	echo "ignite files have been unzipped"
	
	cd ignite1.6/release-package/bin
	chmod +x ignite.sh
	./ignite.sh -J-Xms4g -J-Xmx4g &
	
	echo "Ignite started"
	return 0
}

copy_files()
{
	mkdir /home/hadoop/riskAnalytics
	
	echo "riskAnalytics directory created"
	
	aws s3 cp s3://ignite-binary/riskAnalytics1_lib.zip /home/hadoop/riskAnalytics
	aws s3 cp s3://ignite-binary/data.zip /home/hadoop/riskAnalytics
	
	echo "data and lib files copied"
	
	cd /home/hadoop/riskAnalytics
	unzip riskAnalytics1_lib.zip
	unzip data.zip
	
	echo "data and lib files have been unzipped"
	
	mkdir /mnt/var/log/spark-jobserver
	mkdir /mnt/lib/spark-jobserver
	mkdir /mnt/tmp/spark-jobserver
	
	cd /mnt/lib/spark-jobserver
	aws s3 cp s3://ignite-binary/job-server.tar.gz .
	
	echo "copied job server from s3"
	tar zxf job-server.tar.gz
	
	echo "job server extracted"
	
	./server_start.sh &
	
	echo "job server started"
	
	return 0
}

   
 
 grep '"isMaster": true' /mnt/var/lib/info/instance.json
 
if [ $? -eq 0 ]
then
    echo "Running on Master Node"
    copy_files
else
    echo "Running on Slave Node"
    start_ignite_node
fi

set +x
exit 0
