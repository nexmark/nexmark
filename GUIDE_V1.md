## Nexmark Benchmark Guideline (For V1 runner)

### Requirements

The Nexmark benchmark framework runs Flink queries on [standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.13/ops/deployment/cluster_setup.html), see the Flink documentation for more detailed requirements and how to setup it.

#### Software Requirements

The cluster should consist of **one master node** and **one or more worker nodes**. All of them should be **Linux** environment (the CPU monitor script requries to run on Linux). Please make sure you have the following software installed **on each node**:

- **JDK 1.8.x** or higher (Nexmark scripts uses some tools of JDK),
- **ssh** (sshd must be running to use the Flink and Nexmark scripts that manage
  remote components)

If your cluster does not fulfill these software requirements you will need to install/upgrade it.

Having [**passwordless SSH**](https://linuxize.com/post/how-to-setup-passwordless-ssh-login/) and __the same directory structure__ on all your cluster nodes will allow you to use our scripts to control
everything.

#### Environment Variables

The following environment variable should be set on every node for the Flink and Nexmark scripts.

 - `JAVA_HOME`: point to the directory of your JDK installation.
 - `FLINK_HOME`: point to the directory of your Flink installation.

### Build Nexmark

Before start to run the benchmark, you should build the Nexmark benchmark first to have a benchmark package. Please make sure you have installed `maven` in your build machine. And run the `./build.sh` command under `nexmark-flink` directoy. Then you will get the `nexmark-flink.tgz` archive under the directory.

### Setup Cluster

- Step 1: Download the latest Flink package from the [download page](https://flink.apache.org/downloads.html). Say `flink-<version>-bin-scala_2.11.tgz`.
- Step2: Copy the archives (`flink-<version>-bin-scala_2.11.tgz`, `nexmark-flink.tgz`) to your master node and extract it.
  ```
  tar xzf flink-<version>-bin-scala_2.11.tgz; tar xzf nexmark-flink.tgz
  mv flink-<version> flink; mv nexmark-flink nexmark
  ```
- Step3: Copy the jars under `nexmark/lib` to `flink/lib` which contains the Nexmark source generator.
- Step4: Configure Flink.
  - Edit `flink/conf/workers` and enter the IP address of each worker node. Recommand to set 8 entries.
  - Replace `flink/conf/config.yaml` by `nexmark/conf/config.yaml`. Remember to update the following configurations:
    - Set `jobmanager.rpc.address` to you master IP address
    - Set `state.checkpoints.dir` to your local file path (recommend to use SSD), e.g. `file:///home/username/checkpoint`.
    - Set `state.backend.rocksdb.localdir` to your local file path (recommend to use SSD), e.g. `/home/username/rocksdb`.
- Step5: Configure Nexmark benchmark.
  - Edit `nexmark/conf/nexmark.yaml` and set `nexmark.metric.reporter.host` to your master IP address.
- Step6: Copy `flink` and `nexmark` to your worker nodes using `scp`.
- Step7: Start Flink Cluster by running `flink/bin/start-cluster.sh` on the master node.
- Step8: Setup the benchmark cluster by running `nexmark/bin/setup_cluster.sh` on the master node.
- (If you want to use kafka source instead of datagen source) Step9: Prepare Kafka
  - (Please make sure Flink Kafka Jar is ready in flink/lib/ [download page](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/kafka/#dependencies))
  - Start your kafka cluster. (recommend to use SSD)
  - Create kafka topic: `bin/kafka-topics.sh --create --topic nexmark --bootstrap-server localhost:9092 --partitions 8`.
  - Edit `nexmark/conf/nexmark.yaml`, set `kafka.bootstrap.servers`.
  - Prepare source data: `nexmark/bin/run_query.sh insert_kafka`.
  - NOTE: Kafka source is endless, only supports tps mode (unlimited events.num) now.

### Run Nexmark

You can run the Nexmark benchmark by running `nexmark/bin/run_query.sh all` on the master node. It will run all the queries one by one, and collect benchmark metrics automatically. It will take 50 minutes to finish the benchmark by default. At last, it will print the benchmark summary result (Cores * Time(s) for each query) on the console.

You can also run specific queries by running `nexmark/bin/run_query.sh q1,q2`.

You can also tune the workload of the queries by editing `nexmark/conf/nexmark.yaml` with the `nexmark.workload.*` prefix options.

## Nexmark Benchmark Result

### Machines

Minimum requirements:
- 3 worker node 
- Each machine has 8 cores and 32 GB RAM
- 800 GB SSD local disk

### Flink Configuration

Use the default configuration file `config.yaml` defined in `nexmark-flink/src/main/resources/conf/`.

Some notable configurations including:

- 8 TaskManagers, each has only 1 slot
- 8 GB for each TaskManager and JobManager
- Job parallelism: 8
- Checkpoint enabled with exactly once mode and 3 minutes interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 seconds interval and 50000 rows
- Splitting distinct aggregation optimization is enabled

Flink version: 1.13 ~ 2.0.

### Workloads

Source total events number is 100 million. Source generates 10M records per seconds. The percentage of 3 stream is Bid: 92%, Auction: 6%, Person: 2%.

### Benchmark Results

An example of result table is as follows:

```
+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Nexmark Query     | Events Num        | Cores             | Time(s)           | Cores * Time(s)   | Throughput/Cores  |
+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
|q0                 |100,000,000        |8.45               |76.323             |645.087            |155.02 K/s         |
|q1                 |100,000,000        |8.26               |76.643             |633.165            |157.94 K/s         |
|q2                 |100,000,000        |8.23               |69.309             |570.736            |175.21 K/s         |
|q3                 |100,000,000        |8.59               |76.531             |657.384            |152.12 K/s         |
|q4                 |100,000,000        |12.85              |226.605            |2912.841           |34.33 K/s          |
|q5                 |100,000,000        |10.8               |418.242            |4516.930           |22.14 K/s          |
|q7                 |100,000,000        |14.21              |570.983            |8112.884           |12.33 K/s          |
|q8                 |100,000,000        |9.42               |72.673             |684.288            |146.14 K/s         |
|q9                 |100,000,000        |16.11              |435.882            |7022.197           |14.24 K/s          |
|q10                |100,000,000        |8.09               |213.795            |1729.775           |57.81 K/s          |
|q11                |100,000,000        |10.6               |237.599            |2518.946           |39.7 K/s           |
|q12                |100,000,000        |13.69              |96.559             |1321.536           |75.67 K/s          |
|q13                |100,000,000        |8.24               |92.839             |764.952            |130.73 K/s         |
|q14                |100,000,000        |8.28               |74.861             |620.220            |161.23 K/s         |
|q15                |100,000,000        |8.73               |158.224            |1380.927           |72.42 K/s          |
|q16                |100,000,000        |11.51              |466.008            |5362.602           |18.65 K/s          |
|q17                |100,000,000        |9.24               |92.666             |856.162            |116.8 K/s          |
|q18                |100,000,000        |12.49              |149.076            |1862.171           |53.7 K/s           |
|q19                |100,000,000        |21.38              |106.190            |2270.551           |44.04 K/s          |
|q20                |100,000,000        |17.27              |305.099            |5267.805           |18.98 K/s          |
|q21                |100,000,000        |8.33               |121.845            |1015.293           |98.49 K/s          |
|q22                |100,000,000        |8.25               |93.244             |769.471            |129.96 K/s         |
|Total              |2,200,000,000      |243.029            |4231.196           |51495.920          |1.89 M/s           |
+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
```