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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, DecimalType}
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable

abstract class WindowedAggregateFunction extends AggregateFunction1 {
  self: Product =>

  @transient protected var expr: Expression = null
  @transient protected var aggBase: AggregateExpression1 = null
  @transient override val base: AggregateExpression1 = null

  override def nullable: Boolean = aggBase.nullable

  override def dataType: DataType = aggBase.dataType

  override def update(input: InternalRow): Unit = {}

  def update(inputSeq: Seq[InternalRow]): Unit

  def invUpdate(inputSeq: Seq[InternalRow]): Unit
}

case class WindowedCountFunction() extends WindowedAggregateFunction {
  var count: Long = _

  private[streaming] def init(expr: Expression, aggBase: AggregateExpression1): Unit = {
    this.expr = expr
    this.aggBase = aggBase
  }

  private def end(): Unit = {
    this.expr = null
    this.aggBase = null
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = expr.eval(input)
      if (evaluatedExpr != null) {
        count += 1L
      }
    })
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = expr.eval(input)
      if (evaluatedExpr != null) {
        count -= 1L
      }
    })
    end
  }

  override def eval(input: InternalRow): Any = count
}

case class WindowedSumFunction() extends WindowedAggregateFunction {
  private var sumValue: Any = null

  @transient private var calcType: DataType = null
  @transient private var zero: Cast = null
  @transient private var sum: MutableLiteral = null
  @transient private var addFunction: Coalesce = null
  @transient private var subTractFunction: Coalesce = null

  private[streaming] def init(expr: Expression, aggBase: AggregateExpression1): Unit = {
    this.expr = expr
    this.aggBase = aggBase
    calcType =
      expr.dataType match {
        case DecimalType.Fixed(precision, scale) =>
          DecimalType.bounded(precision + 10, scale)
        case _ =>
          expr.dataType
      }
    zero = Cast(Literal(0), calcType)
    sum = MutableLiteral(sumValue, calcType)
    addFunction = Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(expr, calcType)), sum, zero))
    subTractFunction =
      Coalesce(Seq(Subtract(Coalesce(Seq(sum, zero)), Cast(expr, calcType)), sum, zero))
  }

  private def end(): Unit = {
    sumValue = eval(null)
    this.expr = null
    this.aggBase = null
    calcType = null
    zero = null
    sum = null
    addFunction = null
    subTractFunction = null
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(sum.update(addFunction, _))
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(sum.update(subTractFunction, _))
    end
  }

  override def eval(input: InternalRow): Any = {
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        Cast(sum, dataType).eval(null)
      case _ => sum.eval(null)
    }
  }
}

case class WindowedAverageFunction() extends WindowedAggregateFunction {
  private var sumValue: Any = null
  private var count: Long = _

  @transient private var calcType: DataType = null
  @transient private var sum: MutableLiteral = null

  private[streaming] def init(expr: Expression, aggBase: AggregateExpression1): Unit = {
    this.expr = expr
    this.aggBase = aggBase
    calcType =
      expr.dataType match {
        case DecimalType.Fixed(precision, scale) =>
          DecimalType.bounded(precision + 10, scale)
        case _ =>
          expr.dataType
      }
    sum = sumValue match {
      case null => MutableLiteral(Cast(Literal(0), calcType).eval(null), calcType)
      case _ => MutableLiteral(Cast(Literal(sumValue), calcType).eval(null), calcType)
    }
  }

  private def end(): Unit = {
    sumValue = sum.eval(null)
    this.expr = null
    this.aggBase = null
    calcType = null
    sum = null
  }

  private def addFunction(value: Any) = Add(sum,
    Cast(Literal.create(value, expr.dataType), calcType))

  private def subtractFunction(value: Any) = Subtract(sum,
    Cast(Literal.create(value, expr.dataType), calcType))

  override def eval(input: InternalRow): Any = {
    if (count == 0L) {
      null
    } else {
      expr.dataType match {
        case DecimalType.Fixed(precision, scale) =>
          val dt = DecimalType.bounded(precision + 14, scale + 4)
          Cast(Divide(Cast(sum, dt), Cast(Literal(count), dt)), dataType).eval(null)
        case _ =>
          Divide(
            Cast(sum, dataType),
            Cast(Literal(count), dataType)).eval(null)
      }
    }
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = expr.eval(input)
      if (evaluatedExpr != null) {
        count += 1
        sum.update(addFunction(evaluatedExpr), input)
      }
    })
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = expr.eval(input)
      if (evaluatedExpr != null) {
        count -= 1
        sum.update(subtractFunction(evaluatedExpr), input)
      }
    })
    end
  }
}

case class WindowedMinFunction() extends WindowedAggregateFunction {
  val fat = new FixedSizedAggregator()

  private[streaming] def init(expr: Expression, aggBase: AggregateExpression1): Unit = {
    this.expr = expr
    this.aggBase = aggBase
  }

  private def end(): Unit = {
    this.expr = null
    this.aggBase = null
  }

  private def updateFunc(a: Any, b: Any) = {
    val cmp = GreaterThan(Literal.create(a, expr.dataType), Literal.create(b, expr.dataType))
    if (a == null || cmp.eval(null) == true) b else a
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    val local = MinFunction(expr, aggBase)
    inputSeq.foreach(local.update(_))
    val v = local.eval()

    if (v != null) {
      fat.insert(v, updateFunc)
    }
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    if (inputSeq.length > 0) {
      fat.remove(updateFunc)
    }
    end
  }

  override def eval(input: InternalRow): Any = fat.root
}

case class WindowedMaxFunction() extends WindowedAggregateFunction {
  val fat = new FixedSizedAggregator()

  private[streaming] def init(expr: Expression, aggBase: AggregateExpression1): Unit = {
    this.expr = expr
    this.aggBase = aggBase
  }

  private def end(): Unit = {
    this.expr = null
    this.aggBase = null
  }

  private def updateFunc(a: Any, b: Any) = {
    val cmp = LessThan(Literal.create(a, expr.dataType), Literal.create(b, expr.dataType))
    if (a == null || cmp.eval(null) == true) b else a
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    val local = MaxFunction(expr, aggBase)
    inputSeq.foreach(local.update(_))
    val v = local.eval()

    if (v != null) {
      fat.insert(v, updateFunc)
    }
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    if (inputSeq.length > 0) {
      fat.remove(updateFunc)
    }
    end
  }

  override def eval(input: InternalRow): Any = fat.root
}

case class WindowedCountDistinctFunction() extends WindowedAggregateFunction {
  @transient var exprSeq: Seq[Expression] = null
  @transient var distinctValue: InterpretedProjection = null

  val distinctMap = new mutable.HashMap[InternalRow, Long]


  private[streaming] def init(exprSeq: Seq[Expression], aggBase: AggregateExpression1): Unit = {
    this.exprSeq = exprSeq
    this.aggBase = aggBase
    distinctValue = new InterpretedProjection(exprSeq)
  }

  private def end(): Unit = {
    this.exprSeq = null
    this.aggBase = null
    distinctValue = null
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = distinctValue(input)
      if (!evaluatedExpr.anyNull) {
        if (distinctMap.contains(evaluatedExpr)) {
          distinctMap.put(evaluatedExpr, distinctMap(evaluatedExpr) + 1)
        }
        else {
          distinctMap.put(evaluatedExpr, 1)
        }
      }
    })
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.foreach(input => {
      val evaluatedExpr = distinctValue(input)
      if (!evaluatedExpr.anyNull) {
        if (distinctMap.contains(evaluatedExpr)) {
          distinctMap.put(evaluatedExpr, distinctMap(evaluatedExpr) - 1)
          if (distinctMap(evaluatedExpr) <= 0) {
            distinctMap.remove(evaluatedExpr)
          }
        }
      }
    })
    end
  }

  override def eval(input: InternalRow): Any = distinctMap.size.toLong
}

case class WindowedCombineSetsAndCountFunction() extends WindowedAggregateFunction {
  @transient var inputSet: Expression = null
  val seenMap = new mutable.HashMap[Any, Long]

  private[streaming] def init(inputSet: Expression, aggBase: AggregateExpression1): Unit = {
    this.inputSet = inputSet
    this.aggBase = aggBase
  }

  private def end(): Unit = {
    this.inputSet = null
    this.aggBase = null
  }

  override def update(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.distinct.foreach(input => {
      val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
      val inputIterator = inputSetEval.iterator
      while (inputIterator.hasNext) {
        val value = inputIterator.next
        if (seenMap.contains(value)) {
          seenMap.put(value, seenMap(value) + 1)
        }
        else {
          seenMap.put(value, 1)
        }
      }
    })
    end
  }

  override def invUpdate(inputSeq: Seq[InternalRow]): Unit = {
    inputSeq.distinct.foreach(input => {
      val inputSetEval = inputSet.eval(input).asInstanceOf[OpenHashSet[Any]]
      val inputIterator = inputSetEval.iterator
      while (inputIterator.hasNext) {
        val value = inputIterator.next
        if (seenMap.contains(value)) {
          seenMap.put(value, seenMap(value) - 1)
          if (seenMap(value) <= 0) {
            seenMap.remove(value)
          }
        }
      }
    })
    end
  }

  override def eval(input: InternalRow): Any = seenMap.size.toLong
}

private[streaming] class FixedSizedAggregator() extends Serializable {
  var size = 8
  var arr = new Array[Any](size << 1)
  var front = size
  var back = size - 1
  var len = 0

  def root(): Any = arr(1)

  def nextIdx(idx: Int, array: Array[Any]): Int = {
    if (idx == array.size - 1) array.size >> 1
    else idx + 1
  }

  def resize(newSize: Int, func: (Any, Any) => Any): Unit = {
    val oldArr = arr
    var oldFront = front
    size = newSize
    arr = new Array[Any](size << 1)
    front = size
    back = size - 1
    (0 until len).foreach(i => {
      back = nextIdx(back, arr)
      arr(back) = oldArr(oldFront)
      update(back, func)
      oldFront = nextIdx(oldFront, oldArr)
    })
  }

  def insert(elem: Any, func: (Any, Any) => Any): Unit = {
    back = nextIdx(back, arr)
    arr(back) = elem
    update(back, func)
    len += 1
    if (len == size) { // double size
      resize(size << 1, func)
    }
  }

  def remove(func: (Any, Any) => Any): Unit = {
    arr(front) = null
    update(front, func)
    len -= 1
    front = nextIdx(front, arr)
    if (len > 8 && len < (size >> 2)) { // shrink size
      resize(size >> 1, func)
    }
  }

  def update(index: Int, func: (Any, Any) => Any): Unit = {
    var idx = index
    while (idx != 1) {
      val parent = idx >> 1
      val left = parent << 1
      val right = (parent << 1) + 1
      val result = func(arr(left), arr(right))
      if (arr(parent) == result) {
        idx = 1
      } else {
        arr(parent) = result
        idx = parent
      }
    }
  }
}
