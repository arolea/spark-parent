Spark running on local cluster mode

Add the next lines to /config/spark-eng.sh

export SPARK_WORKER_CORES=3

export SPARK_WORKER_INSTANCES=2

export SPARK_WORKER_MEMORY=3g

See /misc/spark-eng.sh for sample file

Start a master node and two worker nodes via the next commands

start master -> ./sbin/start-master.sh

start slaves -> ./sbin/start-slave.sh spark://127.0.0.1:7077

Check the Spark UI at http://localhost:8080/

Package the application via mvn package, and make sure the path from the Configuration class (ClusterConfig.java) matches your local path

Start the application and visit http://localhost:12345/getMapResult