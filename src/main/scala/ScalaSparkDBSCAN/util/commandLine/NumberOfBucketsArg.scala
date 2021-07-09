package ScalaSparkDBSCAN.util.commandLine

import ScalaSparkDBSCAN.exploratoryAnalysis.ExploratoryAnalysisHelper

private [ScalaSparkDBSCAN] trait NumberOfBucketsArg extends Serializable {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
