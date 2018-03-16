# Spark Streaming Dynamic Resource Manager
[![Build Status](https://travis-ci.com/jupblb/spark-resource-manager.svg?token=Th3w536H1FSWyDtrZS9E&branch=master)](https://travis-ci.com/jupblb/spark-resource-manager)
[![codecov](https://codecov.io/gh/jupblb/spark-resource-manager/branch/master/graph/badge.svg?token=eTpn4ONct7)](https://codecov.io/gh/jupblb/spark-resource-manager)
[![Dependencies](https://app.updateimpact.com/badge/845306779771473920/spark-resource-manager.svg?config=runtime)](https://app.updateimpact.com/latest/845306779771473920/spark-resource-manager)

This is a simple lightweight library for implementing alternative
solutions which are to manage number of executors for a single 
Spark Streaming job. It is meant to be a replacement for current 
`ExecutorAllocationManager` (see 
https://issues.apache.org/jira/browse/SPARK-12133).

## Architecture
As of now it is possible to extend `ResourceManager` abstract class.
By default it can listen to both Spark Core and Spark Streaming events.
Apart from that simple logic with few helper functions is written in order
to make extending it with new code easier.

Parameter configuration can be set inside `spark-defaults.conf` as
well as be added on runtime in a way consistent with developing 
regular Spark application.

## Usage
For usage please see `test` package. Here is a simple example of
NetworkWordCount:

```scala
import org.apache.spark.streaming.implicits._
import com.sap.fugu.FuguResourceManager
...
val conf = new SparkConf().setAppName("NetworkWordCount")

val ssc = new StreamingContext(conf, BatchInterval)
  .withResourceManager(new FuguResourceManager(_))

val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1))
val wordSums = wordCounts.reduceByKey(_ + _)
wordSums.print()

ssc.start()
ssc.awaitTermination()
```

With default configuration such code will kill single executor
every time a batch is finished. Please keep in
mind that killing receivers is recommended.
