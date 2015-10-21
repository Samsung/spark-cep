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

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, SchemaRelationProvider}
import org.apache.spark.sql.streaming.StreamPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

class KafkaSource extends SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {

    require(parameters.contains("topics") &&
      parameters.contains("zkQuorum") &&
      parameters.contains("brokerList") &&
      parameters.contains("messageToRow"))

    val topics = parameters("topics").split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    val kafkaParams = parameters.get("kafkaParams").map { t =>
      t.split(",").map { s =>
        val a = s.split(":")
        (a(0), a(1))
      }.toMap
    }

    val messageToRow = {
      try {
        val clz = Class.forName(parameters("messageToRow"))
        clz.newInstance().asInstanceOf[MessageToRowConverter]
      } catch {
        case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
      }
    }

    val groupId = sqlContext.getConf("spark.streaming.query.id", parameters("groupId"))

    new KafkaRelation(
      parameters("zkQuorum"),
      parameters("brokerList"),
      groupId,
      topics,
      kafkaParams,
      messageToRow,
      schema,
      sqlContext)
  }
}

/**
 * `CREATE [TEMPORARY] TABLE kafkaTable(intField, stringField string...) [IF NOT EXISTS]
 * USING org.apache.spark.sql.streaming.sources.KafkaSource
 * OPTIONS (topics "aa:1,bb:1",
 *   groupId "test",
 *   zkQuorum "localhost:2181",
 *   kafkaParams "sss:xxx,sss:xxx",
 *   messageToRow "xx.xx.xxx")`
 */
case class KafkaRelation(
    zkQuorum: String,
    brokerList: String,
    groupId: String,
    topics: Map[String, Int],
    params: Option[Map[String, String]],
    messageToRowConverter: MessageToRowConverter,
    val schema: StructType,
    @transient val sqlContext: SQLContext)
  extends StreamBaseRelation
  with InsertableRelation
  with StreamPlan {

  private val kafkaParams = params.getOrElse(Map())  ++ Map(
    "zookeeper.connect" -> zkQuorum,
    "group.id" -> groupId,
    "zookeeper.connection.timeout.ms" -> "10000")

  // Currently only support Kafka with String messages
  @transient private lazy val kafkaStream = KafkaUtils.createStream[
    String,
    String,
    StringDecoder,
    StringDecoder
    ](streamSqlContext.streamingContext, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

  @transient lazy val stream: DStream[InternalRow] = kafkaStream.map(_._2)
    .map(messageToRowConverter.toRow(_, schema))

  object KafkaProducer {
    var initialized = false
    var producer: Producer[String, String] = _
    val topic = topics.head._1

    def init() {
      if (initialized == false) {
        val props = new Properties()
        props.put("metadata.broker.list", brokerList)
        props.put("request.required.acks", "-1")
        props.put("compression.codec", "snappy")
        props.put("producer.type", "sync")
        props.put("serializer.class", "kafka.serializer.StringEncoder")
        val config = new ProducerConfig(props)
        producer = new Producer[String, String](config)
        initialized = true
      }
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    assert(validTime != null)

    data.foreachPartition(p => {
      KafkaProducer.init()
      p.foreach(row => {
        val msg = new KeyedMessage[String, String](KafkaProducer.topic,
          messageToRowConverter.toMessage(row))
        try {
          KafkaProducer.producer.send(msg)
        } catch {
          case e: Exception => {
            KafkaProducer.init()
            KafkaProducer.producer.send(msg)
          }
        }
      })
    })
  }
}
