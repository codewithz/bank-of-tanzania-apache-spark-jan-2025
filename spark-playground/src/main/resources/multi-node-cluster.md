=========================================================
Steps to create multi-node cluster on local machine
=========================================================

A. In Command Prompt 1 - Spark Master

A.1. Create Spark master
=> spark-class org.apache.spark.deploy.master.Master --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.master.Master --host localhost

A.2. Copy Master URL and Spark UI URL
- Typically master URL is at port 7077
- Typically Spark UI is at port 4040
- If any of these ports are in use, a different port might be used

----------------------------------------------------------------------------------------------------------

B. In Command Prompt 2 - Worker Node 1

B.1. Create worker node 1
=> spark-class org.apache.spark.deploy.worker.Worker <master URL> --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost

----------------------------------------------------------------------------------------------------------

C. In Command Prompt 3 - Worker Node 2

C.1. Create worker node 2
=> spark-class org.apache.spark.deploy.worker.Worker <master URL> --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.worker.Worker spark://CrystalTalks-PC:7077 --host localhost
----------------------------------------------------------------------------------------------------------

D. Testing multi-node cluster setup with Spark Submit command

D.1. Run spark submit command
=> spark-submit --master <master URL> --name SparkSubmitApp --total-executor-cores 4 --executor-memory 2g --executor-cores 2 "<file path>"

	- Example => spark-submit --master spark://localhost:7077 --name SparkSubmitApp --total-executor-cores 4 --executor-memory 2g --executor-cores 2 "\Spark3Fundamentals\SparkSubmitTest.py