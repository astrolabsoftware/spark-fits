/*
 * Copyright 2018 Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Dependencies._

lazy val sparkVersion = settingKey[String]("Spark version used to compile and test")

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := sys.env.getOrElse("RELEASE_VERSION", "1.0.0-SNAPSHOT"),
     mainClass in Compile := Some("com.astrolabsoftware.sparkfits.ReadFits")
   )),

   //Scala version
   scalaVersion := "2.12.21",
   crossScalaVersions := Seq("2.12.21", "2.13.18"),
   sparkVersion := sys.env.getOrElse("SPARK_VERSION",
     if (scalaBinaryVersion.value == "2.13") "4.2.0" else "3.4.4"),

   // Name of the application
   name := "spark-fits",
   // Name of the orga
   organization := "com.github.astrolabsoftware",
   // Do not execute test in parallel
   parallelExecution in Test := false,
   // Fail the test suite if statement coverage is < 70%
   coverageFailOnMinimum := true,
   coverageMinimumStmtTotal := 70,
   // Put nice colors on the coverage report
   coverageHighlighting := true,
   // Do not publish artifact in test
   publishArtifact in Test := false,
   // Exclude runner class for the coverage
   coverageExcludedPackages := "<empty>;com.astrolabsoftware.sparkfits.ReadFits*;com.astrolabsoftware.sparkfits.ReadImage*",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
   // Shading to avoid conflicts with pre-installed nom.tam.fits library
   // Uncomment if you have such conflicts.
   // assemblyShadeRules in assembly := Seq(ShadeRule.rename("nom.**" -> "new_nom.@1").inAll),
   // Put dependencies of the library
   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
     "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
     scalaTest % Test
   )
 )

// POM settings for Sonatype
description := "FITS data source for Apache Spark"

homepage := Some(
 url("https://github.com/astrolabsoftware/spark-fits")
)
scmInfo := Some(
 ScmInfo(
   url("https://github.com/astrolabsoftware/spark-fits"),
    "scm:git:https://github.com/astrolabsoftware/spark-fits.git"
 )
)

developers := List(
 Developer(
   "JulienPeloton",
   "Julien Peloton",
   "peloton@lal.in2p3.fr",
   url("https://github.com/JulienPeloton")
 )
)

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

publishMavenStyle := true

publishTo := localStaging.value

useGpg := true
