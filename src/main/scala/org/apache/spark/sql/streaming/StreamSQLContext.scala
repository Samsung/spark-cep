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

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources.json.{InferSchema, JacksonParser}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.runtime.universe.TypeTag

/**
 * A component to connect StreamingContext with specific ql context ([[SQLContext]] or
 * [[org.apache.spark.sql.hive.HiveContext]]),
 * offer user the ability to manipulate SQL and LINQ-like query on DStream
 */
class StreamSQLContext(
    val streamingContext: StreamingContext,
    val sqlContext: SQLContext)
  extends Logging {

  // Get internal field of SQLContext to better control the flow.
  protected lazy val catalog = sqlContext.catalog

  // Query parser for streaming specific semantics.
  protected lazy val streamSqlParser = new StreamSQLParser(this)

  // Add stream specific strategy to the planner.
  protected lazy val streamStrategies = new StreamStrategies(sqlContext)
  sqlContext.experimental.extraStrategies = streamStrategies.strategies

  /** udf interface for user to register udf through it */
  val udf = sqlContext.udf

  @transient
  protected[sql] lazy val optimizer: Optimizer = StreamSQLOptimizer

  protected[sql] def executePlan(plan: LogicalPlan) = new this.QueryExecution(plan)

  /**
   * Create a SchemaDStream from a normal DStream of case classes.
   */
  implicit def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowStream = stream.transform(rdd =>
      RDDConversions.productToRowRdd(rdd, schema.fields.map(_.dataType)))
    new SchemaDStream(this, LogicalDStream(attributeSeq, rowStream)(this))
  }

  /**
   * :: DeveloperApi ::
   * Allows catalyst LogicalPlans to be executed as a SchemaDStream. Not this logical plan should
   * be streaming meaningful.
   */
  @DeveloperApi
  implicit def logicalPlanToStreamQuery(plan: LogicalPlan): SchemaDStream =
    new SchemaDStream(this, plan)

  /**
   * :: DeveloperApi ::
   * Creates a [[SchemaDStream]] from and [[DStream]] containing [[Row]]s by applying a schema to
   * this DStream.
   */
  @DeveloperApi
  def createSchemaDStream(rowStream: DStream[InternalRow], schema: StructType): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val attributes = schema.toAttributes
    val logicalPlan = LogicalDStream(attributes, rowStream)(this)
    new SchemaDStream(this, logicalPlan)
  }

  /**
   * Register DStream as a temporary table in the catalog. Temporary table exist only during the
   * lifetime of this instance of sql context.
   */
  def registerDStreamAsTable(stream: SchemaDStream, tableName: String): Unit = {
    catalog.registerTable(Seq(tableName), stream.logicalPlan)
  }

  /**
   * Drop the temporary stream table with given table name in the catalog.
   */
  def dropTable(tableName: String): Unit = {
    catalog.unregisterTable(Seq(tableName))
  }

  /**
   * Returns the specified stream table as a SchemaDStream
   */
  def table(tableName: String): SchemaDStream = {
    new SchemaDStream(this, catalog.lookupRelation(Seq(tableName)))
  }

  /**
   * Execute a SQL or HiveQL query on stream table, returning the result as a SchemaDStream. The
   * actual parser backed by the initialized ql context.
   */
  def sql(sqlText: String): SchemaDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val plan = streamSqlParser.parse(sqlText, false).getOrElse {
      sqlContext.sql(sqlText).queryExecution.logical }
    new SchemaDStream(this, plan)
  }

  /**
   * Execute a command or DDL query and directly get the result (depending on the side effect of
   * this command).
   */
  def command(sqlText: String): String = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    sqlContext.sql(sqlText).collect().map(_.toString()).mkString("\n")
  }

  /**
   * :: Experimental ::
   * Infer the schema from the existing JSON file
   */
  @Experimental
  def inferJsonSchema(path: String, samplingRatio: Double = 1.0): StructType = {
    val colNameOfCorruptedJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
    val jsonRdd = streamingContext.sparkContext.textFile(path)
    InferSchema(jsonRdd, samplingRatio, colNameOfCorruptedJsonRecord)
  }

  /**
   * :: Experimental ::
   * Get a SchemaDStream with schema support from a raw DStream of String,
   * in which each string is a json record.
   */
  @Experimental
  def jsonDStream(json: DStream[String], schema: StructType): SchemaDStream = {
    val colNameOfCorruptedJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
    val rowDStream = json.transform(r => JacksonParser(r, schema, colNameOfCorruptedJsonRecord))
    createSchemaDStream(rowDStream, schema)
  }

  /**
   * :: Experimental ::
   * Infer schema from existing json file with `path` and `samplingRatio`. Get the parsed
   * row DStream with schema support from input json string DStream.
   */
  @Experimental
  def jsonDStream(json: DStream[String], path: String, samplingRatio: Double = 1.0)
    : SchemaDStream = {
    jsonDStream(json, inferJsonSchema(path, samplingRatio))
  }

  /**
   * :: DeveloperApi ::
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  @DeveloperApi
  protected[sql] class QueryExecution(val logical: LogicalPlan) {
    def assertAnalyzed(): Unit = sqlContext.analyzer.checkAnalysis(analyzed)

    lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)
    lazy val withCachedData: LogicalPlan = {
      assertAnalyzed
      sqlContext.cacheManager.useCachedData(analyzed)
    }
    lazy val optimizedPlan: LogicalPlan = optimizer.execute(withCachedData)

    // TODO: Don't just pick the first one...
    lazy val sparkPlan: SparkPlan = {
      SparkPlan.currentContext.set(sqlContext)
      sqlContext.planner.plan(optimizedPlan).next()
    }
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = sqlContext.prepareForExecution.execute(sparkPlan)

    println(toString)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String =
      s"""== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    override def toString: String = {
      def output =
        analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}").mkString(", ")

      s"""== Parsed Logical Plan ==
          |${stringOrError(logical)}
          |== Analyzed Logical Plan ==
          |${stringOrError(output)}
          |${stringOrError(analyzed)}
          |== Optimized Logical Plan ==
          |${stringOrError(optimizedPlan)}
          |== Physical Plan ==
          |${stringOrError(executedPlan)}
          |Code Generation: ${stringOrError(executedPlan.codegenEnabled)}
      """.stripMargin.trim
    }
  }

}
