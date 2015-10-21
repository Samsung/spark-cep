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

package org.apache.spark.sql.streaming.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.streaming.StreamPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

class SocketTextSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {

    require(parameters.contains("host") &&
      parameters.contains("port") &&
      parameters.contains("messageToRow"))

    val messageToRow = {
      try {
        val clz = Class.forName(parameters("messageToRow"))
        clz.newInstance().asInstanceOf[MessageToRowConverter]
      } catch {
        case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
      }
    }

    new SocketTextRelation(
      parameters("host"),
      parameters("port").toInt,
      messageToRow,
      schema,
      sqlContext)
  }
}

/**
 * `CREATE [TEMPORARY] TABLE socketTable(intField, stringField string...) [IF NOT EXISTS]
 * USING org.apache.spark.sql.streaming.sources.SocketTextSource
 * OPTIONS (host "xxx.xxx.xxx.xxx",
 *   port: "xxxx",
 *   messageToRow "xx.xx.xxx")`
 */
case class SocketTextRelation(
    host: String,
    port: Int,
    messageToRowConverter: MessageToRowConverter,
    val schema: StructType,
    @transient val sqlContext: SQLContext)
  extends StreamBaseRelation
  with StreamPlan {

  // Currently only support Kafka with String messages
  @transient private val socketStream = streamSqlContext.streamingContext.socketTextStream(
    host, port)

  @transient val stream: DStream[InternalRow] =
    socketStream.map(messageToRowConverter.toRow(_, schema))
}

