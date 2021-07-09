package ScalaSparkDBSCAN.util.commandLine


private [ScalaSparkDBSCAN] trait NumberOfPointsInPartitionParsing [C <: CommonArgs with NumberOfPointsInPartitionArg]
  extends CommonArgsParser[C] with Serializable {

  opt[Long] ("npp")
    .foreach { args.numberOfPoints = _ }

}


