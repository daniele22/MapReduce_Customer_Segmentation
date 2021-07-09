package ScalaSparkDBSCAN.exploratoryAnalysis

import ScalaSparkDBSCAN.spatial.Point

private [ScalaSparkDBSCAN] class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue)
  extends  Point (pt) with Serializable {

  var distanceToNearestNeighbor = d
}
