package ScalaSparkDBSCAN

import org.apache.spark.{SparkConf, SparkContext}
import ScalaSparkDBSCAN.util.io.IOHelper
import ScalaSparkDBSCAN.util.commandLine._
import ScalaSparkDBSCAN.spatial.rdd.PartitioningSettings
import ScalaSparkDBSCAN.util.debug.{DebugHelper, Clock}

/** A driver program which runs DBSCAN clustering algorithm
 *
 */
object DbscanDriver extends Serializable {

  private [ScalaSparkDBSCAN] class Args (var minPts: Int = DbscanSettings.getDefaultNumberOfPoints,
      var borderPointsAsNoise: Boolean = DbscanSettings.getDefaultTreatmentOfBorderPoints)
      extends CommonArgs with EpsArg with NumberOfPointsInPartitionArg

  private [ScalaSparkDBSCAN] class ArgsParser
    extends CommonArgsParser (new Args (), "DBSCAN clustering algorithm")
    with EpsArgParsing [Args]
    with NumberOfPointsInPartitionParsing [Args] {

    opt[Int] ("numPts")
      .required()
      .foreach { args.minPts = _ }
      .valueName("<minPts>")
      .text("TODO: add description")

    opt[Boolean] ("borderPointsAsNoise")
      .foreach { args.borderPointsAsNoise = _ }
      .text ("A flag which indicates whether border points should be treated as noise")
  }


  def main (args: Array[String]): Unit = {

    val argsParser = new ArgsParser ()

    if (argsParser.parse(args, Config()).isDefined) {

      val clock = new Clock ()


      val conf = new SparkConf()
        .setMaster(argsParser.args.masterUrl)
        .setAppName("DBSCAN")
        .setJars(Array(argsParser.args.jar))

      if (argsParser.args.debugOutputPath.isDefined) {
        conf.set (DebugHelper.DebugOutputPath, argsParser.args.debugOutputPath.get)
      }


      val sc = new SparkContext(conf)

      val (columns, data) = IOHelper.readDataset(sc, argsParser.args.inputPath, false)
      val settings = new DbscanSettings ()
        .withEpsilon(argsParser.args.eps)
        .withNumberOfPoints(argsParser.args.minPts)
        .withTreatBorderPointsAsNoise(argsParser.args.borderPointsAsNoise)
        .withDistanceMeasure(argsParser.args.distanceMeasure)

      val partitioningSettings = new PartitioningSettings (numberOfPointsInBox = argsParser.args.numberOfPoints)

      val clusteringResult = Dbscan.train(data, settings, partitioningSettings)
      IOHelper.saveClusteringResult(clusteringResult, argsParser.args.outputPath)

      clock.logTimeSinceStart("Clustering")
    }
  }
}
