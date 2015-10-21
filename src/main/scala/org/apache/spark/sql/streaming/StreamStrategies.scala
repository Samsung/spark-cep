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

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.{Row, SQLContext, Strategy, execution}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/** Stream related strategies to map stream specific logical plan to physical plan. */
class StreamStrategies(sqlContext: SQLContext) {

  protected def planLater(plan: LogicalPlan) = sqlContext.planner.plan(plan).next()

  def strategies: Seq[Strategy] = StreamStrategy :: Nil

  object StreamStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case LogicalDStream(output, stream) => PhysicalDStream(output, stream) :: Nil
      case x@WindowedLogicalPlan(w, s, child) =>
        WindowedPhysicalPlan(w, s, planLater(child)) :: Nil
      case y@WindowedAggLogicalPlan(g, a, w, s, child) => y match {
        case StreamPartialAggregation(
        namedGroupingAttributes,
        rewrittenAggregateExpressions,
        groupingExpressions,
        partialComputation,
        child) =>
          WindowedAggPhysicalPlan(
            partial = false,
            namedGroupingAttributes, rewrittenAggregateExpressions, w, s,
            execution.Aggregate(
              partial = true,
              groupingExpressions, partialComputation, planLater(child))) :: Nil
        case _ =>
          WindowedAggPhysicalPlan(false, g, a, w, s, planLater(child)) :: Nil
      }
      case l@LogicalRelation(t: StreamPlan) =>
        PhysicalDStream(l.output, t.stream) :: Nil
      case i@logical.InsertIntoTable(
      l@LogicalRelation(t: StreamPlan), part, child, overwrite, ifNotExists) if part.isEmpty =>
        StreamExecutedCommand(InsertIntoStreamSource(l, overwrite), child) :: Nil
      case _ => Nil
    }
  }
}

object StreamPartialAggregation {
  type ReturnType =
  (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case WindowedAggLogicalPlan(groupingExpressions, aggregateExpressions, w, s, child) =>
      // Collect all aggregate expressions.
      val allAggregates =
        aggregateExpressions.flatMap(_ collect { case a: AggregateExpression1 => a})
      // Collect all aggregate expressions that can be computed partially.
      val partialAggregates =
        aggregateExpressions.flatMap(_ collect { case p: PartialAggregate1 => p})

      // Only do partial aggregation if supported by all aggregate expressions.
      if (allAggregates.size == partialAggregates.size) {
        // Create a map of expressions to their partial evaluations for all aggregate expressions.
        val partialEvaluations: Map[TreeNodeRef, SplitEvaluation] =
          partialAggregates.map(a => (new TreeNodeRef(a), a.asPartial)).toMap

        // We need to pass all grouping expressions though so the grouping can happen a second
        // time. However some of them might be unnamed so we alias them allowing them to be
        // referenced in the second aggregation.
        val namedGroupingExpressions: Seq[(Expression, NamedExpression)] =
          groupingExpressions.map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }

        // Replace aggregations with a new expression that computes the result from the already
        // computed partial evaluations and grouping values.
        val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformDown {
          case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
            partialEvaluations(new TreeNodeRef(e)).finalEvaluation

          case e: Expression =>
            namedGroupingExpressions.collectFirst {
              case (expr, ne) if expr semanticEquals e => ne.toAttribute
            }.getOrElse(e)
        }).asInstanceOf[Seq[NamedExpression]]

        val partialComputation = namedGroupingExpressions.map(_._2) ++
          partialEvaluations.values.flatMap(_.partialEvaluations)

        val namedGroupingAttributes = namedGroupingExpressions.map(_._2.toAttribute)

        Some(
          (namedGroupingAttributes,
            rewrittenAggregateExpressions,
            groupingExpressions,
            partialComputation,
            child))
      } else {
        None
      }
    case _ => None
  }
}

object ReverseWindowAggregates extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a@WindowedAggLogicalPlan(groupingExpressions, aggregateExpressions,
    windowDuration, slideDuration, child)
    => Aggregate(groupingExpressions, aggregateExpressions,
      WindowedLogicalPlan(windowDuration, slideDuration, child))
  }
}

trait StreamRunnableCommand extends logical.Command {
  self: Product =>

  def run(sqlContext: SQLContext, validTime: Time, rdd: RDD[InternalRow]): Seq[Row]
}

case class StreamExecutedCommand(cmd: StreamRunnableCommand, query: LogicalPlan)
  extends SparkPlan with StreamPlan {

  val stream = new SchemaDStream(streamSqlContext, ReverseWindowAggregates(query))

  protected[sql] def sideEffectResult: Seq[Row] = {
    assert(validTime != null)
    val rdd = Utils.invoke(classOf[DStream[InternalRow]], stream, "getOrCompute",
      (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[InternalRow]]]
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))

    cmd.run(sqlContext, validTime, rdd)
  }

  override def output = Seq.empty

  override def children = Nil

  override def executeCollect(): Array[Row] = sideEffectResult.toArray

  override def executeTake(limit: Int): Array[Row] = sideEffectResult.take(limit).toArray

  override def doExecute(): RDD[InternalRow] = {
    val convert = CatalystTypeConverters.createToCatalystConverter(schema)
    val converted = sideEffectResult.map(convert(_).asInstanceOf[InternalRow])
    sqlContext.sparkContext.parallelize(converted, 1)
  }
}

private[sql] case class InsertIntoStreamSource(logicalRelation: LogicalRelation,
                                               overwrite: Boolean)
  extends StreamRunnableCommand {

  override def run(sqlContext: SQLContext, validTime: Time, rdd: RDD[InternalRow]) = {

    // Apply the schema of the existing table to the new data.
    val df = sqlContext.internalCreateDataFrame(rdd, logicalRelation.schema)
    logicalRelation.relation.asInstanceOf[StreamPlan].setValidTime(validTime)
    logicalRelation.relation.asInstanceOf[InsertableRelation].insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}
