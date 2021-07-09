package ScalaSparkDBSCAN.spatial

import ScalaSparkDBSCAN.dbscan.{TempPointId, ClusterId}

/** A subclass of [[ScalaSparkDBSCAN.spatial.Point]] used during calculation of clusters within one partition
  *
  * @param p
  */
private [ScalaSparkDBSCAN] class PartiallyMutablePoint (p: Point, val tempId: TempPointId)
  extends Point (p) with Serializable {

  var transientClusterId: ClusterId = p.clusterId
  var visited: Boolean = false

  def toImmutablePoint: Point = new Point (this.coordinates, this.pointId, this.boxId, this.distanceFromOrigin,
    this.precomputedNumberOfNeighbors, this.transientClusterId)

}
