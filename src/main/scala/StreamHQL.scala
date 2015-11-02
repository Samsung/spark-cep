/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties

import kafka.consumer.ConsumerConfig
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.sources.MessageDelimiter
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.RedisManager

import scala.util.parsing.json.JSON

class TabDelimiter extends MessageDelimiter {
  override val delimiter = "\t"
}

object StreamDDL {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val query = args(0)
    val sc = new SparkContext(new SparkConf())
    val ssc = new StreamingContext(sc, Seconds(1))
    val streamSqlContext = new StreamSQLContext(ssc, new HiveContext(sc))
    streamSqlContext.command(query)
    new ConstantInputDStream[Int](ssc, sc.parallelize(Seq(1))).print
    ssc.start()
    ssc.awaitTerminationOrTimeout(100)
    ssc.stop()
  }
}

object StreamHQL {

  object Redis {
    var initialized = false
    var manager: RedisManager = _
    def init(confMap: Map[String, String]) {
      if (initialized == false) {
        manager = new RedisManager(
          confMap("redis.shards"),
          confMap("redis.sentinels"),
          confMap("redis.database").toInt)
        manager.init
        initialized = true
      }
    }
  }

  def removeConsumerGroup(zkQuorum: String, groupId: String) {
    val properties = new Properties()
    properties.put("zookeeper.connect", zkQuorum)
    properties.put("group.id", groupId)
    val conf = new ConsumerConfig(properties)
    val zkClient = new ZkClient(conf.zkConnect)
    zkClient.deleteRecursive(s"/consumers/${conf.groupId}")
    zkClient.close()
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val confMap = JSON.parseFull(args(0)).get.asInstanceOf[Map[String, String]]
    val qid = args(1)
    val query = args(2)
    val sc = new SparkContext(new SparkConf())
    val ssc = new StreamingContext(sc, Seconds(1))
    val hc = new HiveContext(sc)
    val streamSqlContext = new StreamSQLContext(ssc, hc)
    val redisExpireSec = confMap("redis.expire.sec").toInt
    ssc.checkpoint(s"checkpoint/$qid")
    hc.setConf("spark.streaming.query.id", qid)
    hc.setConf("spark.sql.shuffle.partitions", confMap("spark.sql.shuffle.partitions"))

    removeConsumerGroup(confMap("kafka.zookeeper.quorum"), qid)
    val result = streamSqlContext.sql(query)
    val schema = result.schema

    result.foreachRDD((rdd, time) => {
      rdd.foreachPartition(partition => {
        Redis.init(confMap)
        val jedis = Redis.manager.getResource
        val pipe = jedis.pipelined
        partition.foreach(record => {
          val seq = record.toSeq(schema)
          val ts = time.milliseconds / 1000
          val hkey = seq.take(seq.size - 1).mkString(".")
          pipe.hset(qid + "." + ts, hkey, seq(seq.size - 1).toString)
          pipe.expire(qid + "." + ts, redisExpireSec)
        })
        pipe.sync
        Redis.manager.returnResource(jedis)
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
