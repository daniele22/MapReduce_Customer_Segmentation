package ScalaSparkDBSCAN.util.debug

import org.apache.spark.internal.Logging

private [ScalaSparkDBSCAN] class Clock extends Logging with Serializable {
  val startTime = System.currentTimeMillis()

  def logTimeSinceStart (): Unit = {
    logTimeSinceStart("Test")
  }

  def logTimeSinceStart (message: String) = {
    val currentTime = System.currentTimeMillis()
    val timeSinceStart = (currentTime - startTime) / 1000.0

    logInfo (s"$message took $timeSinceStart seconds")
  }
}
