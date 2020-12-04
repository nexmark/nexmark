# Nexmark Benchmark

## What is Nexmark

Nexmark is a benchmark suite for queries over continuous data streams. This project is inspired by the [NEXMark research paper](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/) and [Apache Beam Nexmark](https://beam.apache.org/documentation/sdks/java/testing/nexmark/).

## Nexmark Benchmark Suite

### Schemas

These are multiple queries over a three entities model representing on online auction system:

- **Person** represents a person submitting an item for auction and/or making a bid on an auction.
- **Auction** represents an item under auction.
- **Bid** represents a bid for an item under auction.

### Queries

| Query  | Name | Summary | Flink |
| -------- | -------- | -------- | ------ |
| q0 | Pass Through | Measures the monitoring overhead including the source generator. | ✅ |
| q1 | Currency Conversion | Convert each bid value from dollars to euros. | ✅ |
| q2 | Selection | Find bids with specific auction ids and show their bid price. | ✅ |
| q3 | Local Item Suggestion | Who is selling in OR, ID or CA in category 10, and for what auction ids?  | ✅ |
| q4 | Average Price for a Category | Select the average of the wining bid prices for all auctions in each category. | ✅ |
| q5 | Hot Items | Which auctions have seen the most bids in the last period? | ✅ |
| q6 | Average Selling Price by Seller | What is the average selling price per seller for their last 10 closed auctions. | [FLINK-19059](https://issues.apache.org/jira/browse/FLINK-19059) |
| q7 | Highest Bid | Select the bids with the highest bid price in the last period. | ✅ |
| q8 | Monitor New Users | Select people who have entered the system and created auctions in the last period. | ✅ |
| q9 | Winning Bids | Find the winning bid for each auction. | ✅ |
| q10 | Log to File System | Log all events to file system. Illustrates windows streaming data into partitioned file system. | ✅ |
| q11 | User Sessions | How many bids did a user make in each session they were active? Illustrates session windows. | ✅ |
| q12 | Processing Time Windows | How many bids does a user make within a fixed processing time limit? Illustrates working in processing time window. | ✅ |
| q13 | Bounded Side Input Join | Joins a stream to a bounded side input, modeling basic stream enrichment. | ✅ |
| q14 | Calculation | Convert bid timestamp into types and find bids with specific price. Illustrates more complex projection and filter.  | ✅ |
| q15 | Bidding Statistics Report | How many distinct users join the bidding for different level of price? Illustrates multiple distinct aggregations with filters. | ✅ |

*Note: q1 ~ q8 are from original [NEXMark queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/), q0 and q9 ~ q13 are from [Apache Beam](https://beam.apache.org/documentation/sdks/java/testing/nexmark), others are extended to cover more scenarios.*

### Metrics

For evaluating the performance, there are two performance measurement terms used in Nexmark that are **throughput** and **cores**.

Throughput is the number of events executed by the stream processing system per seconds. As the three entities (Person, Auction, Bid) are generated in a single generator, we measures the total throughput of all events. So that we have a consistent measurement for every queries.
For Flink, we collect the `<source_operator_name>.numRecordsOutPerSecond` metric via Flink [Monitoring REST API](https://ci.apache.org/projects/flink/flink-docs-release-1.11/monitoring/rest_api.html).

Cores is the CPU usage used by the stream processing system. Usually CPU allows preemption, not like memory can be limited. Therefore, how the stream processing system effectively use CPU resources, how much throughput is contributed per core, they are important aspect for streaming performance benchmark.
For Flink, we deploy a CPU usage collector on every worker node and send the usage metric to the benchmark runner for summarizing. We don't use the `Status.JVM.CPU.Load` metric provided by Flink, because it is not accurate.

## Nexmark Benchmark Guideline

### Requirements

The Nexmark benchmark framework runs Flink queries on [standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.11/ops/deployment/cluster_setup.html), see the Flink documentation for more detailed requirements and how to setup it.

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
  - Edit `flink/conf/workers` and enters the IP address of each worker node. Recommand to set 8 entries.
  - Replace `flink/conf/sql-client-defaults.yaml` by `nexmark/conf/sql-client-defaults.yaml`
  - Replace `flink/conf/flink-conf.yaml` by `nexmark/conf/flink-conf.yaml`. Remember to update the following configurations:
    - Set `jobmanager.rpc.address` to you master IP address
    - Set `state.checkpoints.dir` to your local file path (recommend to use SSD), e.g. `file:///home/username/checkpoint`.
    - Set `state.backend.rocksdb.localdir` to your local file path (recommend to use SSD), e.g. `/home/username/rocksdb`.
- Step5: Configure Nexmark benchmark.
  - Set `nexmark.metric.reporter.host` to your master IP address.
- Step6: Copy `flink` and `nexmark` to your worker nodes using `scp`.
- Step7: Start Flink Cluster by running `flink/bin/start-cluster.sh` on the master node.
- Step8: Setup the benchmark cluster by running `nexmark/bin/setup_cluster.sh` on the master node.


### Run Nexmark

You can run the Nexmark benchmark by running `nexmark/bin/run_query.sh all` on the master node. It will run all the queries one by one, and collect benchmark metrics automatically. It will take hours (1h30m) to finish the benchmark by default (6 min for each query). At last, it will print the benchmark summary result (TPS and Cores for each query) on the console.

You can also run specific queries by running `nexmark/bin/run_query.sh q1,q2`.

You can also tune the workload of the queries by editing `nexmark/conf/nexmark.yaml` with the `nexmark.workload.*` prefix options.

## Nexmark Benchmark Result

### Machines

- 3 worker node (ecs.i2g.2xlarge instances on Aliyun)
- Each machine has 1 Xeon 2.5 GHz CPU (8 vCores) and 32 GB RAM
- 800 GB SSD local disk
- 2 Gbps between compute nodes

### Flink Configuration

Use the default configuration file `flink-conf.yaml` and `sql-client-defaults.yaml` defined in `nexmark-flink/src/main/resources/conf/`.

Some notable configurations including:

- 8 TaskManagers, each has only 1 slot
- 4GB for each TaskManager and JobManager
- Job parallelism: 8
- Checkpoint enabled with exactly once mode and 3 minutes interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 seconds interval and 5000 rows
- Splitting distinct aggregation optimization is enabled

Flink version: manually build for release-1.11 branch on commit b9ca9bb.

### Workloads

Source generates 10M records per seconds. The percentage of 3 stream is Bid: 92%, Auction: 6%, Person: 2%.
Each query will warm up for 3 minutes and then collect metrics for 3 minutes.

### Benchmark Results

```
+-------------------+-------------------+-------------------+-------------------+
| Nexmark Query     | Throughput (r/s)  | Cores             | Throughput/Cores  |
+-------------------+-------------------+-------------------+-------------------+
|q0                 |1.9 M              |8.17               |235 K              |
|q1                 |1.8 M              |8.17               |228 K              |
|q2                 |2.1 M              |8.16               |258 K              |
|q3                 |1.9 M              |9.66               |198 K              |
|q4                 |305 K              |11.55              |26 K               |
|q5                 |311 K              |11.71              |26 K               |
|q7                 |153 K              |12.14              |12 K               |
|q8                 |1.8 M              |13.65              |135 K              |
|q9                 |170 K              |11.86              |14 K               |
|q10                |633 K              |8.23               |76 K               |
|q11                |428 K              |10.5               |40 K               |
|q12                |937 K              |12.35              |75 K               |
|q13                |1.4 M              |8.26               |179 K              |
|q14                |1.8 M              |8.28               |228 K              |
|q15                |729 K              |9.06               |80 K               |
+-------------------+-------------------+-------------------+-------------------+
```

## Roadmap

1. Run Nexmark benchmark for more stream processing systems, such as Spark, KSQL. However, they don't have complete streaming SQL features. Therefore, not all of the queries can be ran in these systems. But we can implement the queries in programing way using Spark Streaming, Kafka Streams.
2. Support Latency metric for the benchmark. Latency measures the required time from a record entering the system to some results produced after some actions performed on the record. However, this is not easy to support for SQL queries unless we modify the queries.

## References

- Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier. NEXMark – A Benchmark for Queries over Data Streams. June 2010.

