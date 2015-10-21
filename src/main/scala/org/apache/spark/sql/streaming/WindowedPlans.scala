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

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.dstream.{DStream, WindowedStateDStream}
import org.apache.spark.streaming.{Duration, Time}

/**
 * Logical and physical plan of time-based window.
 */
private[streaming] case class WindowedLogicalPlan(
                                                   windowDuration: Duration,
                                                   slideDuration: Option[Duration],
                                                   child: LogicalPlan)
  extends logical.UnaryNode {
  override def output = child.output
}

private[streaming] case class WindowedPhysicalPlan(
                                                    windowDuration: Duration,
                                                    slideDuration: Option[Duration],
                                                    child: SparkPlan)
  extends execution.UnaryNode with StreamPlan {

  @transient private val wrappedStream = sparkContext.withScope {
    new DStream[InternalRow](streamSqlContext.streamingContext) {
      override def dependencies = parentStreams.toList

      override def slideDuration: Duration = parentStreams.head.slideDuration

      override def compute(validTime: Time): Option[RDD[InternalRow]] = {
        updateChildrenTime(validTime)
        Some(child.execute())
      }

      private lazy val parentStreams = {
        def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
          case x: StreamPlan => x.stream :: Nil
          case _ => plan.children.flatMap(traverse)
        }
        val streams = traverse(child)
        assert(streams.nonEmpty, s"Input query and related plan $child is not a stream plan")
        streams
      }

      def updateChildrenTime(validTime: Time): Unit = {
        def traverse(plan: SparkPlan): Seq[StreamPlan] = plan match {
          case x: StreamPlan => x :: Nil
          case _ => plan.children.flatMap(traverse)
        }
        val streamPlans = traverse(child)
        streamPlans.foreach(_.setValidTime(validTime))
      }
    }
  }

  @transient val stream = slideDuration.map(wrappedStream.window(windowDuration, _))
    .getOrElse(wrappedStream.window(windowDuration))

  override def output = child.output

  override def doExecute() = {
    assert(validTime != null)
    Utils.invoke(classOf[DStream[InternalRow]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[InternalRow]]]
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }
}

private[streaming] case class WindowedAggLogicalPlan(groupingExpressions: Seq[Expression],
                                                     aggregateExpressions: Seq[NamedExpression],
                                                     windowDuration: Duration,
                                                     slideDuration: Option[Duration],
                                                     child: LogicalPlan)
  extends logical.UnaryNode {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}


case class State(count: Long, buffer: Array[AggregateFunction1])

private[streaming] case class WindowedAggPhysicalPlan(
                                                       partial: Boolean,
                                                       groupingExpressions: Seq[Expression],
                                                       aggregateExpressions: Seq[NamedExpression],
                                                       windowDuration: Duration,
                                                       slideDuration: Option[Duration],
                                                       child: SparkPlan)
  extends execution.UnaryNode with StreamPlan {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }
  }

  case class ComputedAggregate(
                                unbound: AggregateExpression1,
                                aggregate: AggregateExpression1,
                                resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression1 =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction1] = {
    val buffer = new Array[AggregateFunction1](computedAggregates.length)
    computedAggregates.indices.foreach(i => {
      val aggregate = computedAggregates(i).aggregate
      aggregate match {
        case a@Count(child) => buffer(i) = new WindowedCountFunction()
        case a@Sum(child) => buffer(i) = new WindowedSumFunction()
        case a@Average(child) => buffer(i) = new WindowedAverageFunction()
        case a@Min(child) => buffer(i) = new WindowedMinFunction()
        case a@Max(child) => buffer(i) = new WindowedMaxFunction()
        case a@CountDistinct(child) => buffer(i) = new WindowedCountDistinctFunction()
        case a@CombineSetsAndCount(child) => buffer(i) = new WindowedCombineSetsAndCountFunction()
        case _ => buffer(i) = aggregate.newInstance()
      }
    })
    buffer
  }

  /** Initialize aggregate buffer for a group. */
  private[this] def initAggregateBuffer(buffer: Array[AggregateFunction1]): Unit = {
    computedAggregates.indices.foreach(i => {
      val aggregate = computedAggregates(i).aggregate
      aggregate match {
        case a@Count(child) => buffer(i).asInstanceOf[WindowedCountFunction].init(child, a)
        case a@Sum(child) => buffer(i).asInstanceOf[WindowedSumFunction].init(child, a)
        case a@Average(child) => buffer(i).asInstanceOf[WindowedAverageFunction].init(child, a)
        case a@Min(child) => buffer(i).asInstanceOf[WindowedMinFunction].init(child, a)
        case a@Max(child) => buffer(i).asInstanceOf[WindowedMaxFunction].init(child, a)
        case a@CountDistinct(child) =>
          buffer(i).asInstanceOf[WindowedCountDistinctFunction].init(child, a)
        case a@CombineSetsAndCount(child) =>
          buffer(i).asInstanceOf[WindowedCombineSetsAndCountFunction].init(child, a)
        case _ =>
      }
    })
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  @transient private val wrappedStream =
    new DStream[InternalRow](streamSqlContext.streamingContext) {

    override def dependencies = parentStreams.toList

    override def slideDuration: Duration = parentStreams.head.slideDuration

    override def compute(validTime: Time): Option[RDD[InternalRow]] = {
      updateChildrenTime(validTime)
      Some(child.execute())
    }

    private lazy val parentStreams = {
      def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
        case x: StreamPlan => x.stream :: Nil
        case _ => plan.children.flatMap(traverse)
      }
      val streams = traverse(child)
      assert(streams.nonEmpty, s"Input query and related plan $child is not a stream plan")
      streams
    }

    def updateChildrenTime(validTime: Time): Unit = {
      def traverse(plan: SparkPlan): Seq[StreamPlan] = plan match {
        case x: StreamPlan => x :: Nil
        case _ => plan.children.flatMap(traverse)
      }
      val streamPlans = traverse(child)
      streamPlans.foreach(_.setValidTime(validTime))
    }
  }

  @transient val stream = aggregateStream(wrappedStream)

  val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)
  val aggregateResults = new GenericMutableRow(computedAggregates.length)
  val resultProjection = new InterpretedMutableProjection(
    resultExpressions, computedSchema ++ namedGroups.map(_._2))
  val joinedRow = new JoinedRow

  def aggregateStream(parent: DStream[InternalRow]): DStream[InternalRow] = {
    val updateFunc = (values: Seq[InternalRow], state: Option[State]) => {
      val prevState = state.getOrElse(State(0L, newAggregateBuffer()))
      initAggregateBuffer(prevState.buffer)
      prevState.buffer.foreach {
        case waf: WindowedAggregateFunction => waf.update(values)
        case af => values.foreach(af.update)
      }
      Some(State(prevState.count + values.length, prevState.buffer))
    }
    val invUpdateFunc = (values: Seq[InternalRow], state: Option[State]) => {
      val prevState = state.getOrElse(State(0L, newAggregateBuffer()))
      initAggregateBuffer(prevState.buffer)
      prevState.buffer.foreach {
        case waf: WindowedAggregateFunction => waf.invUpdate(values)
        case _ =>
      }
      Some(State(prevState.count - values.length, prevState.buffer))
    }
    def newFunc(func: (Seq[InternalRow], Option[State]) => Some[State])
    : (Iterator[(InternalRow, Seq[InternalRow], Option[State])]) =>
      Iterator[(InternalRow, State)] = {
      val cleanedUpdateF = sparkContext.clean(func)
      (iterator: Iterator[(InternalRow, Seq[InternalRow], Option[State])]) => {
        iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
      }
    }
    val filterFunc = (x: (InternalRow, State)) => x._2.count != 0L

    sparkContext.withScope {
      new WindowedStateDStream(
        parent.map(e => (groupingProjection(e).copy, e.copy)),
        sparkContext.clean(newFunc(updateFunc)),
        sparkContext.clean(newFunc(invUpdateFunc)), windowDuration,
        slideDuration.getOrElse(windowDuration),
        new HashPartitioner(sqlContext.conf.numShufflePartitions),
        true,
        Some(sparkContext.clean(filterFunc)),
        Some(new EmptyRDD[(InternalRow, State)](sparkContext))
      )
    }.map(e => {
      initAggregateBuffer(e._2.buffer)
      e._2.buffer.indices.foreach(i => aggregateResults(i) = e._2.buffer(i).eval(EmptyRow))
      resultProjection(joinedRow(aggregateResults, e._1))
    })
  }

  override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    assert(validTime != null)
    Utils.invoke(classOf[DStream[InternalRow]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[InternalRow]]]
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }

}

