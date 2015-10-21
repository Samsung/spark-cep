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

import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class JoinTestSuite extends FunSuite with Eventually with BeforeAndAfter with Logging {

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var sqlc: SQLContext = null
  private var streamQlContext: StreamSQLContext = null

  def beforeFunction() {
    val conf = new SparkConf().setAppName("streamSQLTest").setMaster("local[4]")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(spark.util.Utils.createTempDir().getAbsolutePath())
    sqlc = new SQLContext(sc)
    streamQlContext = new StreamSQLContext(ssc, sqlc)
    sqlc.setConf("spark.sql.shuffle.partitions", "10")
  }

  def afterFunction() {
    if (ssc != null) {
      ssc.stop()
    }
  }

  before(beforeFunction)
  after(afterFunction)

  private def createStreamingTable(
                                    streamSQLContext: StreamSQLContext,
                                    sqlc: SQLContext,
                                    ssc: StreamingContext,
                                    jsonPath: String,
                                    tableName: String) = {
    val schema = streamSQLContext.inferJsonSchema(jsonPath)
    val dstream = new SimpleJsonFileInputDStream(sqlc, ssc, jsonPath)
    streamSQLContext.registerDStreamAsTable(
      streamSQLContext.jsonDStream(dstream, schema), tableName)
  }

  test("test streaming table join streaming table join RDD table with window function") {
    createStreamingTable(streamQlContext, sqlc, ssc,
      "src/test/resources/registration.json", "registration")
    createStreamingTable(streamQlContext, sqlc, ssc, "src/test/resources/student.json", "student")
    val teacherDF = sqlc.read.json("src/test/resources/teacher.json")
    sqlc.registerDataFrameAsTable(teacherDF, "teacher")
    var resultList = ListBuffer[String]()
    val dstream = streamQlContext.sql(
      """
         SELECT
             avg(r.score),
             s.name,
             t.name
         FROM
             student OVER(WINDOW '2' SECONDS, SLIDE '1' SECONDS)  s,
             registration  OVER(WINDOW '2' SECONDS, SLIDE '1' SECONDS)  r,
             teacher  t
         WHERE
             r.studentId = s.id
             and r.teacherId = t.id
         GROUP BY
             r.score,
             s.name,
             t.name
      """)
    dstream.foreachRDD { rdd =>
      rdd.collect().foreach { row =>
        resultList += row.toSeq(dstream.schema).mkString(",")
      }
    }
    ssc.start()

    val expectedResult = List("60.0,jack,bing", "80.0,jack,bing", "70.0,lucy,google", "80.0,lucy,google")
    eventually(timeout(5000 milliseconds), interval(500 milliseconds)) {
      assert(resultList.size > 0)
      assert(resultList.forall(expectedResult.contains(_)),
        "the sql result should be within the expected result set")
    }
  }

  test("test streaming table join RDD table") {
    createStreamingTable(streamQlContext, sqlc, ssc, "src/test/resources/registration.json",
      "registration")
    val teacherDF = sqlc.read.json("src/test/resources/teacher.json")
    sqlc.registerDataFrameAsTable(teacherDF, "teacher")
    val resultList = ListBuffer[String]()
    val dstream = streamQlContext.sql(
      """
         SELECT
             r.className,
             t.name
         FROM
             registration r,
             teacher  t
         WHERE
             r.teacherId = t.id
      """)
    dstream.foreachRDD { rdd =>
      rdd.collect().foreach { row =>
        resultList += row.toSeq(dstream.schema).mkString(",")
      }
    }
    ssc.start()
    val expectedResult = List("math,bing", "english,bing", "math,google", "english,google")
    eventually(timeout(5000 milliseconds), interval(500 milliseconds)) {
      assert(resultList == expectedResult)
    }
  }
}
