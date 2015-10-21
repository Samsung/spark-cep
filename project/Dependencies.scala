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

import sbt._

object Dependencies {

  import Properties._

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-sql"  % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-catalyst" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-hive" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-streaming" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % SPARK_VERSION,
    "redis.clients" % "jedis" % "2.6.2",
    "org.apache.commons" % "commons-pool2" % "2.2",
    "mysql" % "mysql-connector-java" % "5.1.16"
  ).map(_.excludeAll(
      ExclusionRule(organization = "asm")
    )
  )

  lazy val testDeps = Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "junit" % "junit" % "4.10" % "test"
  )

  val repos = Seq(
    "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
    "Maven Repository" at "http://repo1.maven.org/maven2",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io",
    "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
  )
}
