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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.ConstantInputDStream

object WordCountQuery {
  case class SingleWord(word: String)

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    val dummyRDD = sc.parallelize(1 to 10).map(i => SingleWord(s"$i"))
    val dummyStream = new ConstantInputDStream[SingleWord](ssc, dummyRDD)
    registerDStreamAsTable(dummyStream, "test")
    sql(
      """
        |SELECT t.word, COUNT(t.word)
        |FROM (SELECT * FROM test) OVER (WINDOW '9' SECONDS, SLIDE '3' SECONDS) AS t
        |GROUP BY t.word
      """.stripMargin)
      .map(_.copy()).print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(18 * 1000)
    ssc.stop()
  }
}
