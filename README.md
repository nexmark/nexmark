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
| q16 | Channel Statistics Report | How many distinct users join the bidding for different level of price for a channel? Illustrates multiple distinct aggregations with filters for multiple keys. | ✅ |
| q17 | Auction Statistics Report | How many bids on an auction made a day and what is the price? Illustrates an unbounded group aggregation. | ✅ |
| q18 | Find last bid | What's a's last bid for bidder to auction? Illustrates a Deduplicate query. | ✅ |
| q19 | Auction TOP-10 Price | What's the top price 10 bids of an auction? Illustrates a TOP-N query. | ✅ |
| q20 | Expand bid with auction | Get bids with the corresponding auction information where category is 10. Illustrates a filter join. | ✅ |
| q21 | Add channel id | Add a channel_id column to the bid table. Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL. | ✅ |
| q22 | Get URL Directories | What is the directory structure of the URL? Illustrates a SPLIT_INDEX SQL. | ✅ |

*Note: q1 ~ q8 are from original [NEXMark queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/), q0 and q9 ~ q13 are from [Apache Beam](https://beam.apache.org/documentation/sdks/java/testing/nexmark), others are extended to cover more scenarios.*

### Metrics

For evaluating the performance, there are two performance measurement terms used in Nexmark that are **cores** and **time**.

Cores is the CPU usage used by the stream processing system. Usually CPU allows preemption, not like memory can be limited. Therefore, how the stream processing system effectively use CPU resources, how much throughput is contributed per core, they are important aspect for streaming performance benchmark.
For Flink, we deploy a CPU usage collector on every worker node and send the usage metric to the benchmark runner for summarizing. We don't use the `Status.JVM.CPU.Load` metric provided by Flink, because it is not accurate.

Time is the cost time for specified number of events executed by the stream processing system. With Cores * Time, we can know how many resources the stream processing system uses to process specified number of events.

## Nexmark Benchmark Guideline

For the Nexmark benchmark, we provide a guideline to help you run the benchmark on Flink.
Currently, we have two versions of the Nexmark benchmark runner, named V1 and V2 respectively.
The V1 runner is the original version of the Nexmark benchmark runner, where each query contains one dataset. The framework can evaluate in either throughput or throughput per core in heavy load or daily load.
The V2 runner is a new introduced two-phase benchmark runner, the main difference is that the V2 runner only evaluates the performance of latter phase of each query, which is more accurate to evaluate the performance of the long-running queries. Only heavy load with data generator source supported.
For more details of V2, please read [the proposal of New Runner](https://github.com/nexmark/nexmark/issues/65).

Select one of the versions to run the benchmark according to your requirements.
 - [Nexmark Benchmark V1 Guideline](./GUIDE_V1.md)
 - [Nexmark Benchmark V2 Guideline](./GUIDE_V2.md)

## Roadmap

1. Run Nexmark benchmark for more stream processing systems, such as Spark, KSQL. However, they don't have complete streaming SQL features. Therefore, not all of the queries can be ran in these systems. But we can implement the queries in programing way using Spark Streaming, Kafka Streams.
2. Support Latency metric for the benchmark. Latency measures the required time from a record entering the system to some results produced after some actions performed on the record. However, this is not easy to support for SQL queries unless we modify the queries.

## References

- Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier. NEXMark – A Benchmark for Queries over Data Streams. June 2010.

