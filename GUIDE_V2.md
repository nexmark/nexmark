## Nexmark Benchmark Guideline (For V2 runner)

### Requirements

The Nexmark benchmark framework runs Flink queries on [standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.13/ops/deployment/cluster_setup.html), see the Flink documentation for more detailed requirements and how to setup it.

#### Software Requirements

The cluster should consist of **one master node** and **one or more worker nodes**. All of them should be **Linux** environment (the CPU monitor script requries to run on Linux). Please make sure you have the following software installed **on each node**:

- **JDK 1.11.x** or higher (Nexmark scripts uses some tools of JDK),
- **ssh** (sshd must be running to use the Flink and Nexmark scripts that manage
  remote components)
- **Flink 2.0.0** or above.
- **DFS environment** (HDFS, S3, etc.) is necessary for the checkpointing and state backend.

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
    - Set `execution.checkpointing.dir` to DFS path, e.g. `hdfs:///checkpoints/`.
    - Set a proper state backend via `state.backend.type`, e.g. `forst` or `rocksdb`.
    - Set `io.tmp.dirs` to your local file path (recommend to use SSD), e.g. `/mnt/disk1/tmp`.
- Step5: Configure Nexmark benchmark.
  - Replace `nexmark/conf/nexmark.yaml` by `nexmark/conf/nexmark_v2.yaml`.
  - Edit `nexmark/conf/nexmark.yaml` and set `nexmark.metric.reporter.host` to your master IP address.
- Step6: Copy `flink` and `nexmark` to your worker nodes using `scp`.
- Step7: Start Flink Cluster by running `flink/bin/start-cluster.sh` on the master node.
- Step8: Setup the benchmark cluster by running `nexmark/bin/setup_cluster.sh` on the master node.

### Run Nexmark

You can run the Nexmark benchmark by running `nexmark/bin/run_query.sh all` on the master node. It will run all the queries one by one, and collect benchmark metrics automatically. It will take 50 minutes to finish the benchmark by default. At last, it will print the benchmark summary result (Cores * Time(s) for each query) on the console.

You can also run specific queries by running `nexmark/bin/run_query.sh q1,q2`.

You can also tune the workload of the queries by editing `nexmark/conf/nexmark.yaml` with the `nexmark.workload.*` prefix options.

## Nexmark Benchmark Result

### Machines

Minimum requirements:
- 4 worker node 
- Each machine has 16 cores and 32 GB RAM
- 100 GB SSD local disk (or less for disaggregated state)
- HDFS with 4 data nodes with SSD disks on 1Gbps LAN

### Flink Configuration

Use the default configuration file `config_v2.yaml` defined in `nexmark-flink/src/main/resources/conf/`.

Some notable configurations including:

- 8 TaskManagers, each has only 1 slot
- 8 GB for each TaskManager and JobManager
- Job parallelism: 8
- Checkpoint enabled with exactly once mode and 30 seconds interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 seconds interval and 50000 rows
- Splitting distinct aggregation optimization is enabled

Flink version: 2.0 or above.

### Workloads

Source total events number is 200 million (150M for warmup and 50M for evaluation). Source generates 100K records per seconds. The percentage of 3 stream is Bid: 92%, Auction: 6%, Person: 2%.

### Benchmark Results

An example of result table is as follows:

```
+------+-----------------+--------+----------+--------------+-------------------+
| Query| Events Num      | Cores  | Time(s)  | Throughput   | Recovery Time(s)  |
+------+-----------------+--------+----------+--------------+-------------------+
|q0    |50,000,000       |7       |18.875    |2.65 M/s      |0.004              |
|q1    |50,000,000       |7.24    |17.462    |2.86 M/s      |0.002              |
|q2    |50,000,000       |7.13    |16.927    |2.95 M/s      |0.002              |
|q3    |50,000,000       |8.69    |26.250    |1.9 M/s       |0.002              |
|q4    |50,000,000       |12.33   |143.745   |347.84 K/s    |0.002              |
|q5    |50,000,000       |12.35   |54.324    |920.4 K/s     |0.001              |
|q7    |50,000,000       |11.45   |190.508   |262.46 K/s    |3.001              |
|q8    |50,000,000       |12.83   |23.994    |2.08 M/s      |0.004              |
|q9    |50,000,000       |10.86   |334.395   |149.52 K/s    |9.009              |
|q10   |50,000,000       |6.81    |55.290    |904.32 K/s    |0.002              |
|q11   |50,000,000       |9.29    |116.856   |427.88 K/s    |0.002              |
|q12   |50,000,000       |10.03   |25.656    |1.95 M/s      |0.001              |
|q13   |50,000,000       |7.97    |33.326    |1.5 M/s       |0.003              |
|q14   |50,000,000       |7.83    |24.660    |2.03 M/s      |0.001              |
|q15   |50,000,000       |8.16    |36.419    |1.37 M/s      |0.002              |
|q16   |50,000,000       |10.77   |102.608   |487.29 K/s    |0.002              |
|q17   |50,000,000       |10.15   |30.599    |1.63 M/s      |0.003              |
|q18   |50,000,000       |11.77   |64.954    |769.77 K/s    |3.017              |
|q19   |50,000,000       |13.09   |89.471    |558.84 K/s    |3.003              |
|q20   |50,000,000       |10.98   |256.461   |194.96 K/s    |6.035              |
|q21   |50,000,000       |7.81    |43.492    |1.15 M/s      |0.001              |
|q22   |50,000,000       |7.7     |30.725    |1.63 M/s      |0.001              |
|Total |1,100,000,000    |212.24  |1736.997  |28.7 M/s      |24.1               |
+------+-----------------+--------+----------+--------------+-------------------+
```