package ScalaSparkDBSCAN.spatial

import ScalaSparkDBSCAN._
import ScalaSparkDBSCAN.dbscan.BoxId

private [ScalaSparkDBSCAN] class BoxIdGenerator (val initialId: BoxId) extends Serializable {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
