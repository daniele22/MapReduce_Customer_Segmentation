package ScalaSparkDBSCAN.util.commandLine

private [ScalaSparkDBSCAN] trait NumberOfBucketsArgParsing [C <: CommonArgs with NumberOfBucketsArg]
  extends CommonArgsParser[C] with Serializable {

  opt[Int] ("numBuckets")
    .foreach { args.numberOfBuckets = _ }
    .valueName("<numBuckets>")
    .text("Number of buckets in a histogram")
}
