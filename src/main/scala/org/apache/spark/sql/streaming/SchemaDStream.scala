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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.CreateTableUsingAsSelect
import org.apache.spark.sql.execution.{ExplainCommand, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
 * :: Experimental ::
 * A row based DStream with schema involved, offer user the ability to manipulate SQL or
 * LINQ-like query on DStream, it is similar to SchemaRDD, which offers the similar function to
 * users. Internally, SchemaDStream treat rdd of each batch duration as a small table, and force
 * query on this small table.
 *
 * The SQL function offered by SchemaDStream is a subset of standard SQL or HiveQL, currently it
 * doesn't support INSERT, CTAS like query in which queried out data need to be written into another
 * destination.
 */
@Experimental
class SchemaDStream(
                     @transient val streamSqlContext: StreamSQLContext,
                     @transient val queryExecution: StreamSQLContext#QueryExecution)
  extends DStream[InternalRow](streamSqlContext.streamingContext) {

  def this(streamSqlContext: StreamSQLContext, logicalPlan: LogicalPlan) =
    this(streamSqlContext, streamSqlContext.executePlan(logicalPlan))

  override def dependencies = parentStreams.toList

  override def slideDuration: Duration = parentStreams.head.slideDuration

  override def compute(validTime: Time): Option[RDD[InternalRow]] = {
    // Set the valid batch duration for this rule to get correct RDD in DStream of this batch
    // duration
    updateChildrenTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    Some(queryExecution.executedPlan.execute())
  }

  // To guard out some unsupported logical plans.
  @transient private[streaming] val logicalPlan: LogicalPlan = queryExecution.logical match {
    case _: Command |
         _: InsertIntoTable |
         _: CreateTableUsingAsSelect => {
      LogicalDStream(queryExecution.analyzed.output, this)(streamSqlContext)
      //      throw new IllegalStateException(s"logical plan ${queryExecution.logical} " +
      //        s"is not supported currently")
    }
    case _ =>
      queryExecution.logical
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.stream :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    val streams = traverse(queryExecution.executedPlan)
    assert(!streams.isEmpty, s"Input query and related plan ${queryExecution.executedPlan}" +
      s" is not a stream plan")
    streams
  }

  def updateChildrenTime(validTime: Time): Unit = {
    def traverse(plan: SparkPlan): Seq[StreamPlan] = plan match {
      case x: StreamPlan => x :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    val streamPlans = traverse(queryExecution.executedPlan)
    streamPlans.foreach(_.setValidTime(validTime))
  }

  /**
   * Returns the schema of this SchemaDStream (represented by a [[StructType]]).
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Register itself as a temporary stream table.
   */
  def registerAsTable(tableName: String): Unit = {
    streamSqlContext.registerDStreamAsTable(this, tableName)
  }

  /**
   * Explain the query to get logical plan as well as physical plan.
   */
  def explain(extended: Boolean): Unit = {
    streamSqlContext.logicalPlanToStreamQuery(
      ExplainCommand(queryExecution.logical, extended = extended))
      .queryExecution.executedPlan.executeCollect().map {
      r => println(r.getString(0))
    }
  }
}
