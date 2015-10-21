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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.ConstantInputDStream

object UdtfEnabledQuery {

  case class People(name: String, items: Array[String])

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext
    val hiveContext = new HiveContext(sc)
    val streamSqlContext = new StreamSQLContext(ssc, hiveContext)
    import hiveContext.implicits._
    import streamSqlContext.createSchemaDStream
    val dummyRDD = sc.makeRDD(1 to 3).map(i => People(s"jack$i", Array("book", "gun")))
    val dummyStream = new ConstantInputDStream[People](ssc, dummyRDD)
    streamSqlContext.registerDStreamAsTable(dummyStream, "people")
    streamSqlContext.sql(
      """SELECT
        |    name,
        |    item
        |FROM
        |    people
        |    lateral view explode(items) items AS item""".stripMargin).map(_.copy()).print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }
}
