name := "MapReduce_Customer_Segmentation"

version := "0.1"

scalaVersion := "2.12.8"


// SPARK PACKAGES
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "compile"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2" % "compile"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0" % "compile"

// evilplot
resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"

// needed for sbt-assembly
val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
