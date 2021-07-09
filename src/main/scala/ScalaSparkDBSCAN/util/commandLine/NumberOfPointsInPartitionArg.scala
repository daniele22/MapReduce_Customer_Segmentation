package ScalaSparkDBSCAN.util.commandLine

import ScalaSparkDBSCAN.spatial.rdd.PartitioningSettings


private [ScalaSparkDBSCAN] trait NumberOfPointsInPartitionArg extends Serializable {
  var numberOfPoints: Long = PartitioningSettings.DefaultNumberOfPointsInBox
}
