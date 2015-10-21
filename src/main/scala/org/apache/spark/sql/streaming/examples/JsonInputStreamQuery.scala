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

import scala.collection.mutable.SynchronizedQueue
import scala.io.Source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}


object JsonInputStreamQuery {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext
    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._
    // Here we read data line by line from a given file and then put it into a queue DStream.
    // You can replace any kind of String type DStream here including kafka DStream.
    val queue = new SynchronizedQueue[RDD[String]]()
    Source.fromFile("src/main/resources/student.json").getLines().foreach(msg =>
      queue.enqueue(sc.parallelize(List(msg))))
    val queueDStream = ssc.queueStream[String](queue)
    // We can infer the schema of json automatically by using inferJsonSchema
    val schema = streamSqlContext.inferJsonSchema("src/main/resources/student.json")
    streamSqlContext.registerDStreamAsTable(
      streamSqlContext.jsonDStream(queueDStream, schema), "jsonTable")
    sql("SELECT * FROM jsonTable").print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }
}
