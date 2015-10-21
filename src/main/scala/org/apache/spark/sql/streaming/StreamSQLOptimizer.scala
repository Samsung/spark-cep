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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.streaming.Duration

object WindowAggregates extends Rule[LogicalPlan] {

  var windowDuration: Duration = _
  var slideDuration: Option[Duration] = _

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a@Aggregate(groupingExpressions, aggregateExpressions, child)
    => {
      initDurations()
      val newChild = getNewPlan(child)
      if (areDurationsNotNull && isAvailableAggEx(aggregateExpressions, groupingExpressions)) {
        WindowedAggLogicalPlan(groupingExpressions, aggregateExpressions, windowDuration,
          slideDuration, newChild)
      }
      else a
    }
  }

  private def initDurations(): Unit = {
    windowDuration = null
    slideDuration = null
  }

  private def getNewPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case WindowedLogicalPlan(windowDuration, slideDuration, child) => {
      if (areDurationsNotNull
        && (this.windowDuration != windowDuration || this.slideDuration != slideDuration)) {
        this.windowDuration = null
        this.slideDuration = null
        child
      }
      else {
        this.windowDuration = windowDuration
        this.slideDuration = slideDuration
        getNewPlan(child)
      }
    }
    case _ => {
      if (plan.children.isEmpty) return plan
      val newChildren: Seq[LogicalPlan] = plan.children.map(p => getNewPlan(p))
      plan.withNewChildren(newChildren)
    }
  }

  private def areDurationsNotNull = (windowDuration != null && slideDuration != null)

  private def isAvailableAggEx(aggregateExpressions: Seq[NamedExpression],
                               groupingExpressions: Seq[Expression]): Boolean = {
    val aggs = aggregateExpressions.filter(!groupingExpressions.contains(_))
    val aggExs = aggs.flatMap(agg => agg.collect { case a: AggregateExpression => a })
    aggExs.foreach(x => x match {
      case Count(_) | CombineSetsAndCount(_) | Sum(_)
           | Average(_) | Min(_) | Max(_) | CountDistinct(_) =>
      case _ => return false
    })
    true
  }
}

object StreamSQLOptimizer extends Optimizer {

  val batches =
  // SubQueries are only needed for analysis and can be removed before execution.
    Batch("Remove SubQueries", FixedPoint(100),
      EliminateSubQueries) ::
      Batch("Aggregate", FixedPoint(100),
        ReplaceDistinctWithAggregate,
        RemoveLiteralFromGroupExpressions) ::
      Batch("Operator Optimizations", FixedPoint(100),
        // Operator push down
        SetOperationPushDown,
        SamplePushDown,
        PushPredicateThroughJoin,
        PushPredicateThroughProject,
        PushPredicateThroughGenerate,
        ColumnPruning,
        // Operator combine
        ProjectCollapsing,
        CombineFilters,
        CombineLimits,
        // Constant folding
        NullPropagation,
        OptimizeIn,
        ConstantFolding,
        LikeSimplification,
        BooleanSimplification,
        RemovePositive,
        SimplifyFilters,
        SimplifyCasts,
        SimplifyCaseConversionExpressions) ::
      Batch("Decimal Optimizations", FixedPoint(100),
        DecimalAggregates) ::
      Batch("LocalRelation", FixedPoint(100),
        ConvertToLocalRelation) ::
      Batch("Aggregate Optimizations", FixedPoint(100),
        WindowAggregates) ::
      Nil
}
