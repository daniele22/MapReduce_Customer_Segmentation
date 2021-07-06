name := "MapReduce_Customer_Segmentation"

version := "0.1"

scalaVersion := "2.12.0"


//TODO check compile or provided

// SPARK PACKAGES
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.2" % "compile"
