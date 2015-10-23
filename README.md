# Spark CEP
Spark CEP is a stream processing engine for Apache Spark with built-in continuous query language.

In the absence of SQL-like query engine for streams and the lack of fundamental functionality in alternatives which is based on the conjunction of Spark Streaming and Spark SQL, needs for stream processing engine along proper supports to the stream come to the fore.


##Quick Start

###Creating StreamSQLContext

StreamSQLContext is the main entry point for all streaming sql related functionalities. StreamSQLContext can be created by:

```scala
val ssc: StreamingContext
val sqlContext: SQLContext
    
val streamSqlContext = new StreamSQLContext(ssc, sqlContext)
```

Or you could use HiveContext to get full Hive semantics support, like:
    
```scala
val ssc: StreamingContext
val hiveContext: HiveContext

val streamSqlContext = new StreamSQLContext(ssc, hiveContext)
```

###Running SQL on DStreams
    
```scala
case class Person(name: String, age: String)

// Create an DStream of Person objects and register it as a stream.
val people: DStream[Person] = ssc.socketTextStream(serverIP, serverPort)
  .map(_.split(","))
  .map(p => Person(p(0), p(1).toInt))
    
val schemaPeopleStream = streamSqlContext.createSchemaDStream(people)
schemaPeopleStream.registerAsTable("people")
    
val teenagers = sql("SELECT name FROM people WHERE age >= 10 && age <= 19")
    
// The results of SQL queries are themselves DStreams and support all the normal operations
teenagers.map(t => "Name: " + t(0)).print()
ssc.start()
ssc.awaitTerminationOrTimeout(30 * 1000)
ssc.stop()
```

###Stream Relation Join

```scala    
val userStream: DStream[User]
streamSqlContext.registerDStreamAsTable(userStream, "user")
    
val itemStream: DStream[Item]
streamSqlContext.registerDStreamAsTable(itemStream, "item")
    
sql("SELECT * FROM user JOIN item ON user.id = item.id").print()
    
val historyItem: DataFrame
historyItem.registerTempTable("history")
sql("SELECT * FROM user JOIN item ON user.id = history.id").print()
```

###Time Based Windowing Join/Aggregation

```scala
sql(
  """
    |SELECT t.word, COUNT(t.word)
    |FROM (SELECT * FROM test) OVER (WINDOW '9' SECONDS, SLIDE '3' SECONDS) AS t
    |GROUP BY t.word
  """.stripMargin)

sql(
  """
    |SELECT * FROM
    |  user1 OVER (WINDOW '9' SECONDS, SLIDE '6' SECONDS) AS u
    |JOIN
    |  user2 OVER (WINDOW '9' SECONDS, SLIDE '6' SECONDS) AS v
        |ON u.id = v.id
        |WHERE u.id > 1 and u.id < 3 and v.id > 1 and v.id < 3
      """.stripMargin)
```

Note: For time-based windowing join, the sliding size should be same for all the joined streams. This is the limitation of Spark Streaming.

###External Source API Support for Kafka

```scala
streamSqlContext.command(
      s"""
         |CREATE TEMPORARY TABLE t_kafka (
         |word string
         |,num int
         |)
         |USING org.apache.spark.sql.streaming.sources.KafkaSource
         |OPTIONS(
         |zkQuorum "10.10.10.1:2181",
         |brokerList "10.10.10.1:9092,10.10.10.2:9092",
         |groupId  "test",
         |topics   "aa:10",
         |messageToRow "org.apache.spark.sql.streaming.sources.MessageDelimiter")
      """.stripMargin)
```

###How to Build and Deploy

Spark CEP is built with sbt, you could use sbt related commands to test/compile/package.

Spark CEP is built on >= Spark-1.5, you could change the Spark version in Build.scala to the version you wanted, currently Spark CEP can be worked with Spark version 1.5+.

To use Spark CEP, put the packaged jar into your environment where Spark could access, you could use spark-submit --jars or other ways.


####Spark CEP job submission sample

```bash
{$SPARK_HOME}/bin/spark-submit \
     --class StreamHQL \
     --name "CQLDemo" \
     --master yarn-cluster \
     --num-executors 4 \
     --driver-memory 256m \
     --executor-memory 512m \
     --executor-cores 1 \
     --conf spark.default.parallelism=5 \
     lib/spark-cep-assembly-0.1.0-SNAPSHOT.jar \
     "{ \
    \"kafka.zookeeper.quorum\": \"10.10.10.1:2181\", \
    \"redis.shards\": \"shard1\",\
    \"redis.sentinels\": \"10.10.10.2:26379\",\
    \"redis.database\": \"0\", \
    \"redis.expire.sec\": \"600\", \
    \"spark.sql.shuffle.partitions\": \"10\" \
    }" \
    sample_query \
    SELECT COUNT(DISTINCT t.duid) FROM stream_test OVER (WINDOW '300' SECONDS, SLIDE '5' SECONDS) AS t
```

There are few arguments being passed to the Spark CEP job.
First, it requires zookeeper url(`kafka.zookeeper.quorum`) for consuming stream from Kafka.
Since it stores the result within a window to redis, it also requires Redis connection information.
You can pass continuous query against a Kafka topic(`stream_test`).

If you want to contribute our project, please refer to [Governance](https://github.com/samsung/spark-cep/wiki/Governance)
___
Contact: [Robert B. Kim](byungjin.kim@samsung.com), [Junseok Heo](jun.seok.heo@samsung.com)
___
