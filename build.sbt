import Dependencies._

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     scalaVersion := "2.11.8",
     version      := "0.1.0",
     mainClass in Compile := Some("sparkfits.ReadFits")
   )),
   name := "toto",
   parallelExecution in Test := false,
   coverageFailOnMinimum := true,
   coverageHighlighting := true,
   coverageMinimum := 70,
   publishArtifact in Test := false,
   libraryDependencies ++= Seq(
     "gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.15.2",
     "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
     scalaTest % Test
   )
 )

//  // META-INF discarding
// mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//    {
//     case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//     case x => MergeStrategy.first
//    }
// }

//"gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.15.2",
