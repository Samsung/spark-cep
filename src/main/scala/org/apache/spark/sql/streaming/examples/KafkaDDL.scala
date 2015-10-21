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

package org.apache.spark.sql.streaming.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaDDL {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[10]").setAppName("test")
    val sc = new SparkContext(sconf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(spark.util.Utils.createTempDir().getAbsolutePath())
    val sqc = new SQLContext(sc)
    val streamSqlContext = new StreamSQLContext(ssc, sqc)

    streamSqlContext.command("SET spark.sql.shuffle.partitions = 10")
    streamSqlContext.command(
      """
        |CREATE TEMPORARY TABLE t_kafka (
        |  word string
        |  ,num int
        |)
        |USING org.apache.spark.sql.streaming.sources.KafkaSource
        |OPTIONS(
        |  zkQuorum "localhost:2181",
        |  brokerList "localhost:9092",
        |  groupId  "test",
        |  topics   "aa:1",
        |  messageToRow "org.apache.spark.sql.streaming.sources.MessageDelimiter")
      """.stripMargin)

    streamSqlContext.sql(
      """
        |SELECT t.word, COUNT(t.word), SUM(t.num), AVG(t.num), MAX(t.num), MIN(t.num)
        |FROM (SELECT * FROM t_kafka) OVER (WINDOW '15' SECONDS, SLIDE '3' SECONDS) AS t
        |GROUP BY t.word
      """.stripMargin).map(_.copy).print

    ssc.start()
    ssc.awaitTerminationOrTimeout(600 * 1000)
    ssc.stop()
  }
}

object KafkaHQL {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sconf = new SparkConf().setMaster("local[10]").setAppName("test")

    val sc = new SparkContext(sconf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val qid = "cql0"
    val hc = new HiveContext(sc)
    val streamSqlContext = new StreamSQLContext(ssc, hc)
    ssc.checkpoint(spark.util.Utils.createTempDir().getAbsolutePath())
    hc.setConf("spark.streaming.query.id", qid)

    streamSqlContext.command("SET spark.sql.shuffle.partitions = 1")
    streamSqlContext.command(
      """
        |CREATE TABLE IF NOT EXISTS kafka (
        |  word string
        |  ,num bigint
        |)
        |USING org.apache.spark.sql.streaming.sources.KafkaSource
        |OPTIONS(
        |  zkQuorum "localhost:2181",
        |  brokerList "localhost:9092",
        |  groupId  "test",
        |  topics   "aa:1",
        |  messageToRow "org.apache.spark.sql.streaming.sources.MessageDelimiter")
      """.stripMargin)

    streamSqlContext.command(
      """
        |CREATE TABLE IF NOT EXISTS kafka2 (
        |  word string
        |  ,num bigint
        |)
        |USING org.apache.spark.sql.streaming.sources.KafkaSource
        |OPTIONS(
        |  zkQuorum "localhost:2181",
        |  brokerList "localhost:9092",
        |  groupId  "test",
        |  topics   "bb:1",
        |  messageToRow "org.apache.spark.sql.streaming.sources.MessageDelimiter")
      """.stripMargin)

    streamSqlContext.sql(
      """
        |INSERT INTO TABLE kafka2
        |SELECT t.word, COUNT(t.word)
        |FROM (SELECT * FROM kafka) OVER (WINDOW '15' SECONDS, SLIDE '3' SECONDS) AS t
        |GROUP BY t.word
      """.stripMargin).map(_.copy).print

    ssc.start()
    ssc.awaitTerminationOrTimeout(600 * 1000)
    ssc.stop()
  }

}
