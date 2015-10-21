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

package org.apache.spark.streaming.dstream

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.reflect.ClassTag

class WindowedStateDStream[K: ClassTag, V: ClassTag, S: ClassTag]
(
  @transient parent: DStream[(K, V)],
  updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
  invUpdateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
  _windowDuration: Duration,
  _slideDuration: Duration,
  partitioner: Partitioner,
  preservePartitioning: Boolean = true,
  filterFunc: Option[((K, S)) => Boolean] = None,
  initialRDD: Option[RDD[(K, S)]] = None
  )
  extends DStream[(K, S)](parent.context) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(storageLevel: StorageLevel): DStream[(K, S)] = {
    super.persist(storageLevel)
    parent.persist(storageLevel)
    this
  }

  private[this] def computeUsingPreviousRDD(newRDD: RDD[(K, V)], prevStateRDD: RDD[(K, S)]) = {
    // Define the function for the mapPartition operation on cogrouped RDD;
    // first map the cogrouped tuple to tuples of required type,
    // and then apply the update function
    val updateFuncLocal = updateFunc
    val finalFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
      val i = iterator.map(t => {
        val itr = t._2._2.iterator
        val headOption = if (itr.hasNext) Some(itr.next()) else None
        (t._1, t._2._1.toSeq, headOption)
      })
      updateFuncLocal(i)
    }
    val cogroupedRDD = newRDD.cogroup(prevStateRDD, partitioner)
    val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
    Some(stateRDD)
  }

  private[this] def invComputeUsingOldRDD(oldRDD: RDD[(K, V)], prevStateRDD: RDD[(K, S)]) = {
    val invUpdateFuncLocal = invUpdateFunc
    val finalInvFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
      val i = iterator.map(t => {
        val itr = t._2._2.iterator
        val headOption = if (itr.hasNext) Some(itr.next()) else None
        (t._1, t._2._1.toSeq, headOption)
      })
      invUpdateFuncLocal(i)
    }

    val cogroupedRDD = oldRDD.cogroup(prevStateRDD, partitioner)
    val stateRDD = cogroupedRDD.mapPartitions(finalInvFunc, preservePartitioning)
    Some(stateRDD)
  }

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {
    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime)
    val previousWindow = currentWindow - slideDuration
    val oldRDD = new UnionRDD(ssc.sc,
      parent.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration))
      .partitionBy(partitioner)
    val newRDD = new UnionRDD(ssc.sc,
      parent.slice(previousWindow.endTime + parent.slideDuration, currentWindow.endTime))
      .partitionBy(partitioner)

    // Try to get the previous state RDD
    val stateRDD = getOrCompute(currentTime - slideDuration) match {

      case Some(prevStateRDD) => {
        // If previous state RDD exists
        // Try to get the parent RDD
        newRDD.isEmpty match {
          case false => {
            // If parent RDD exists, then compute as usual
            val computedRDD =
              if (!oldRDD.isEmpty) invComputeUsingOldRDD(oldRDD, prevStateRDD).get
              else prevStateRDD
            computeUsingPreviousRDD(newRDD, computedRDD)
          }
          case true => {
            // If parent RDD does not exist
            // Re-apply the update function to the old state RDD
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq[V](), Option(t._2)))
              updateFuncLocal(i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            if (!oldRDD.isEmpty) invComputeUsingOldRDD(oldRDD, stateRDD) else Some(stateRDD)
          }
        }
      }

      case None => {
        // If previous session RDD does not exist (first input data)

        // Try to get the parent RDD
        newRDD.isEmpty match {
          case false => {
            // If parent RDD exists, then compute as usual
            initialRDD match {
              case None => {
                // Define the function for the mapPartition operation on grouped RDD;
                // first map the grouped tuple to tuples of required type,
                // and then apply the update function
                val updateFuncLocal = updateFunc
                val finalFunc = (iterator: Iterator[(K, Iterable[V])]) => {
                  updateFuncLocal(iterator.map(tuple => (tuple._1, tuple._2.toSeq, None)))
                }

                val groupedRDD = newRDD.groupByKey(partitioner)
                val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
                // logDebug("Generating state RDD for time " + validTime + " (first)")
                Some(sessionRDD)
              }
              case Some(initialStateRDD) => {
                val computedRDD =
                  if (!oldRDD.isEmpty) invComputeUsingOldRDD(oldRDD, initialStateRDD).get
                  else initialStateRDD
                computeUsingPreviousRDD(newRDD, computedRDD)
              }
            }
          }
          case true => {
            // If parent RDD does not exist, then nothing to do!
            // logDebug("Not generating state RDD (no previous state, no parent)")
            initialRDD
          }
        }
      }
    }

    if (filterFunc.isDefined && stateRDD.isDefined) {
      Some(stateRDD.get.filter(filterFunc.get))
    } else {
      stateRDD
    }

  }
}
