package ScalaSparkDBSCAN.util.commandLine

import org.apache.commons.math3.ml.distance.DistanceMeasure
import ScalaSparkDBSCAN.DbscanSettings

private [ScalaSparkDBSCAN] class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure,
  var debugOutputPath: Option[String] = None)
  extends Serializable
