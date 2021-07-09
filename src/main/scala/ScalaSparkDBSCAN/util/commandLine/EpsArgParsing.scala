package ScalaSparkDBSCAN.util.commandLine

private [ScalaSparkDBSCAN] trait EpsArgParsing [C <: CommonArgs with EpsArg]
  extends CommonArgsParser[C] with Serializable {

  opt[Double] ('e', "eps")
    .required()
    .foreach { args.eps = _ }
    .valueName("<eps>")
    .text("Distance within which points are considered close enough to be assigned to one cluster")
}
