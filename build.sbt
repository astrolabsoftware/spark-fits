import Dependencies._

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := "0.1.0",
     mainClass in Compile := Some("com.sparkfits.ReadFits")
   )),
   name := "spark-fits",
   parallelExecution in Test := false,
   coverageFailOnMinimum := true,
   coverageHighlighting := true,
   coverageMinimum := 70,
   publishArtifact in Test := false,
   coverageExcludedPackages := "<empty>;com.sparkfits.ReadFits*",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
   libraryDependencies ++= Seq(
     "gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.15.2",
     "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
     scalaTest % Test
   )
 )
