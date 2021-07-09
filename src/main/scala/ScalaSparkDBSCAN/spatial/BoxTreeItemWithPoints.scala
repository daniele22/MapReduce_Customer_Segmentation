package ScalaSparkDBSCAN.spatial

import ScalaSparkDBSCAN.util.collection.SynchronizedArrayBuffer

private [ScalaSparkDBSCAN] class BoxTreeItemWithPoints (b: Box,
  val points: SynchronizedArrayBuffer[Point] = new SynchronizedArrayBuffer[Point] (),
  val adjacentBoxes: SynchronizedArrayBuffer[BoxTreeItemWithPoints] = new SynchronizedArrayBuffer[BoxTreeItemWithPoints] ())
  extends BoxTreeItemBase [BoxTreeItemWithPoints] (b) with Serializable {
}
