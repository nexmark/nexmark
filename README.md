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

For evaluating the performance, there are two performance measurement terms used in Nexmark that are **cores** and **time**.

Cores is the CPU usage used by the stream processing system. Usually CPU allows preemption, not like memory can be limited. Therefore, how the stream processing system effectively use CPU resources, how much throughput is contributed per core, they are important aspect for streaming performance benchmark.
For Flink, we deploy a CPU usage collector on every worker node and send the usage metric to the benchmark runner for summarizing. We don't use the `Status.JVM.CPU.Load` metric provided by Flink, because it is not accurate.

Time is the cost time for specified number of events executed by the stream processing system. With Cores * Time, we can know how many resources the stream processing system uses to process specified number of events.

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

Use the default configuration file `flink-conf.yaml` and `sql-client-defaults.yaml` defined in `nexmark-flink/src/main/resources/conf/`.

Some notable configurations including:

- 8 TaskManagers, each has only 1 slot
- 8 GB for each TaskManager and JobManager
- Job parallelism: 8
- Checkpoint enabled with exactly once mode and 3 minutes interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 seconds interval and 5000 rows
- Splitting distinct aggregation optimization is enabled

Flink version: 1.13.

### Workloads

Source total events number is 100 million. Source generates 10M records per seconds. The percentage of 3 stream is Bid: 92%, Auction: 6%, Person: 2%.

### Benchmark Results

```
+--------------+------ ---------+----------+-----------+-------------------+-------------------+
|Nexmark Query | Events Num     | Cores    | Time(s)   | Cores * Time(s)   | Throughput/Cores  |
|--------------|----------------|----------|-----------|-------------------|-------------------|
|q0            |100,000,000     |8.34      |48.663     |405.793            |246 K              |
|q1            |100,000,000     |8.4       |49.614     |416.639            |240 K              |
|q2            |100,000,000     |8.24      |45.256     |372.985            |268 K              |
|q3            |100,000,000     |9.07      |62.640     |568.090            |176 K              |
|q4            |100,000,000     |12.28     |218.609    |2683.576           |37.2 K             |
|q5            |100,000,000     |10.73     |368.410    |3954.738           |25.2 K             |
|q7            |100,000,000     |13.91     |564.737    |7856.976           |12.7 K             |
|q8            |100,000,000     |9.93      |58.513     |581.232            |172 K              |
|q9            |100,000,000     |14.01     |359.642    |5038.584           |19.8 K             |
|q10           |100,000,000     |8.22      |200.426    |1647.809           |60.6 K             |
|q11           |100,000,000     |10.08     |243.547    |2455.134           |40.7 K             |
|q12           |100,000,000     |12.8      |96.226     |1231.649           |81.1 K             |
|q13           |100,000,000     |8.18      |83.391     |682.280            |146 K              |
|q14           |100,000,000     |8.19      |64.923     |532.002            |187 K              |
|q15           |100,000,000     |8.57      |141.839    |1215.683           |82.2 K             |
|Total         |1,500,000,000   |150.962   |2606.436   |29643.170          |1.8 M             |
+--------------+------ ---------+----------+-----------+-------------------+-------------------+
```

## Roadmap

1. Run Nexmark benchmark for more stream processing systems, such as Spark, KSQL. However, they don't have complete streaming SQL features. Therefore, not all of the queries can be ran in these systems. But we can implement the queries in programing way using Spark Streaming, Kafka Streams.
2. Support Latency metric for the benchmark. Latency measures the required time from a record entering the system to some results produced after some actions performed on the record. However, this is not easy to support for SQL queries unless we modify the queries.

## References

- Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier. NEXMark – A Benchmark for Queries over Data Streams. June 2010.

