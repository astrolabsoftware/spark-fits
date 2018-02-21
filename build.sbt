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

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := "0.1.0",
     mainClass in Compile := Some("com.sparkfits.ReadFits")
   )),
   // Name of the application
   name := "spark-fits",
   // Do not execute test in parallel
   parallelExecution in Test := false,
   // Fail the test suite if statement coverage is < 70%
   coverageFailOnMinimum := true,
   coverageMinimum := 70,
   // Put nice colors on the coverage report
   coverageHighlighting := true,
   // Do not publish artifact in test
   publishArtifact in Test := false,
   // Exclude runner class for the coverage
   coverageExcludedPackages := "<empty>;com.sparkfits.ReadFits*",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
   // Shading to avoid conflicts with pre-installed nom.tam.fits library
   // Uncomment if you have such conflicts.
   // assemblyShadeRules in assembly := Seq(ShadeRule.rename("nom.**" -> "new_nom.@1").inAll),
   // Put dependencies of the library
   libraryDependencies ++= Seq(
     "gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.15.2",
     "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
     scalaTest % Test
   )
 )
