name := "MapReduce_Customer_Segmentation"

version := "0.1"

scalaVersion := "2.12.8"


//TODO check compile or provided

// SPARK PACKAGES
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2" % "compile"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.2" % "compile"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2" % "compile"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0" % "compile"

//// https://mvnrepository.com/artifact/org.plotly-scala/plotly-core
//libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.8.0" % "compile"
//// https://mvnrepository.com/artifact/org.plotly-scala/plotly-render
//libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.0"

// https://mvnrepository.com/artifact/org.vegas-viz/vegas
//libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"

//libraryDependencies  ++= Seq(
//  // Last stable release
//  "org.scalanlp" %% "breeze" % "1.0",
//
//  // The visualization library is distributed separately as well.
//  // It depends on LGPL code
//  "org.scalanlp" %% "breeze-viz" % "1.0"
//)

//libraryDependencies += "com.github.haifengl" %% "smile-scala" % "2.6.0"
//libraryDependencies += "com.github.haifengl" %% "smile-plot" % "2.6.0"

resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"
