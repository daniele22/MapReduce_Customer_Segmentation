package ScalaSparkDBSCAN.util.commandLine

import ScalaSparkDBSCAN.DbscanSettings

private [ScalaSparkDBSCAN] trait EpsArg extends Serializable {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
