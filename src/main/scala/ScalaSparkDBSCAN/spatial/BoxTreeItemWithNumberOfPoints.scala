package ScalaSparkDBSCAN.spatial

import scala.collection.mutable.ArrayBuffer

private [ScalaSparkDBSCAN] class BoxTreeItemWithNumberOfPoints (b: Box)
  extends BoxTreeItemBase [BoxTreeItemWithNumberOfPoints] (b) with Serializable {

  var numberOfPoints: Long = 0

  override def clone (): BoxTreeItemWithNumberOfPoints  = {

    val result = new BoxTreeItemWithNumberOfPoints (this.box)
    result.children = this.children.map { x => x.clone () }.toList

    result
  }

}
