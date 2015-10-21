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

package org.apache.spark.sql.streaming

import scala.io.Source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

class SimpleJsonFileInputDStream (
    sqlc: SQLContext,
    @transient ssc: StreamingContext,
    path: String) extends InputDStream[String](ssc) {
  val jsons = Source.fromFile(path).getLines().toList
  var index = 0

  override def start(): Unit = {
  }
  override def stop(): Unit =  {
  }
  override def compute(validTime: Time): Option[RDD[String]] = {
    val rddOption = Option(ssc.sparkContext.parallelize(List(jsons(index  % jsons.size))))
    index = index + 1
    rddOption
  }
}
