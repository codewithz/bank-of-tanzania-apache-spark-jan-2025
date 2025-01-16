
# Steps to Connect Spark on One Machine to HBase on Another

## 1. Set Up HBase
- Ensure the HBase cluster is properly configured and running on the remote machine.
- Verify HBase accessibility from the Spark machine:
  ```bash
  telnet <hbase-machine-ip> 2181
  ```
  If the connection succeeds, your Spark machine can communicate with HBase via Zookeeper.

## 2. Ensure HBase Configuration is Available to Spark
The `hbase-site.xml` file is critical as it tells Spark how to locate and connect to the HBase cluster.

### Option A: Use `hbase-site.xml` Locally
- Copy the `hbase-site.xml` file from the HBase cluster to the Spark machine.
  ```bash
  scp <hbase-user>@<hbase-machine>:/path/to/hbase-site.xml /path/to/spark/conf/
  ```
- Place it in `$SPARK_HOME/conf` or the `src/main/resources` folder of your application.

### Option B: Distribute `hbase-site.xml` Dynamically
- Use `spark-submit` to distribute the file:
  ```bash
  spark-submit \
  --class com.codewithz.HBaseReadExample \
  --master spark://<spark-master-ip>:7077 \
  --files /path/to/hbase-site.xml \
  target/hbase-spark-example-1.0.jar
  ```
- Modify the code to explicitly load the file:
  ```java
  SparkSession spark = SparkSession.builder()
      .appName("HBase Remote Connection")
      .config("spark.hadoop.hbase.configuration", "./hbase-site.xml")
      .getOrCreate();
  ```

## 3. Verify Zookeeper Configuration in `hbase-site.xml`
Ensure `hbase-site.xml` on the Spark machine has the correct Zookeeper configuration pointing to the HBase cluster:

```xml
<configuration>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value><hbase-machine-ip></value> <!-- Comma-separated for multiple nodes -->
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value> <!-- Default Zookeeper port -->
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
</configuration>
```

## 4. Include the HBase Spark Connector
Ensure the HBase Spark connector is included in your Spark application dependencies.

### Maven Example:
```xml
<dependency>
  <groupId>org.apache.hbase.connectors.spark</groupId>
  <artifactId>hbase-spark</artifactId>
  <version>2.5.4</version> <!-- Compatible with HBase and Spark versions -->
</dependency>
```

## 5. Configure HBase Table Schema in Code
The table schema and connection logic remain the same as when Spark and HBase are on the same machine. Here's an example:

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;

public class HBaseRemoteExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("HBase Remote Example")
            .master("local[*]") // Change to Spark cluster if needed
            .getOrCreate();

        // HBase table schema
        String hbaseTableSchema = "{"
                + ""table":{"namespace":"default", "name":"test_table"},"
                + ""rowkey":"key","
                + ""columns":{"
                + ""col1":{"cf":"cf", "col":"col1", "type":"string"},"
                + ""col2":{"cf":"cf", "col":"col2", "type":"int"}"
                + "}"
                + "}";

        // Read data from HBase
        Dataset<Row> hbaseDF = spark.read()
            .option(HBaseTableCatalog.tableCatalog(), hbaseTableSchema)
            .format("org.apache.hadoop.hbase.spark")
            .load();

        // Show the data
        hbaseDF.show();
    }
}
```

## 6. Allow Network Access Between Machines
- Open the required ports on the HBase machine:
  - Zookeeper (default: 2181)
  - HBase Master Web UI (default: 16010)
  - HBase REST API (if used, default: 8080)

- Ensure the Spark machine can access the HBase Zookeeper quorum:
  ```bash
  telnet <hbase-machine-ip> 2181
  ```

## 7. Run the Spark Application
Use `spark-submit` to run the application:
```bash
spark-submit \
--class com.codewithz.HBaseRemoteExample \
--master spark://<spark-master-ip>:7077 \
--jars /path/to/hbase-spark.jar \
--files /path/to/hbase-site.xml \
target/hbase-spark-example-1.0.jar
```
