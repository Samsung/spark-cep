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
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin
import scalariform.formatter.preferences._

object Properties {
  val SPARK_VERSION = "1.5.1"
}

object StreamSQLBuild extends Build {

  import Dependencies._

  lazy val root = Project(id = "spark-cep", base = file("."),
    settings = commonSettings ++ Seq(
      description := "An extension of Spark Streaming to support SQL-based query processing",
      libraryDependencies ++= sparkDeps ++ testDeps,
      parallelExecution in Test := false)
  )

  lazy val runScalaStyle = taskKey[Unit]("testScalaStyle")

  // rat task need to be added later.
  lazy val runRat = taskKey[Unit]("run-rat-task")
  lazy val runRatTask = runRat:= {
    "bin/run-rat.sh" !
  }

  lazy val commonSettings = Seq(
    organization := "spark.cep",
    version      := "0.1.0-SNAPSHOT",
    crossPaths   := false,
    scalaVersion := "2.10.4",
    scalaBinaryVersion := "2.10",
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",

    runScalaStyle := {
      org.scalastyle.sbt.PluginKeys.scalastyle.toTask("").value
    },

    (compile in Compile) <<= (compile in Compile) dependsOn runScalaStyle,

    scalacOptions := Seq("-deprecation",
      "-feature",
      "-language:implicitConversions",
      "-language:postfixOps"),
    resolvers ++= Dependencies.repos,
    parallelExecution in Test := false
  ) ++ scalariformPrefs ++ ScalastylePlugin.Settings ++ mergeStrategySetting

  lazy val scalariformPrefs = defaultScalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, false)
  )

  lazy val mergeStrategySetting = Seq(
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
      case PathList("scala",  _ @ _*) => MergeStrategy.first
      case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.first
      case x if x.startsWith("META-INF/mailcap") => MergeStrategy.first
      case x if x.startsWith("plugin.properties") => MergeStrategy.first
      case x => (mergeStrategy in assembly).value(x)
    }
  )

}
