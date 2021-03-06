package ScalaSparkDBSCAN.util

import java.lang.Math

import ScalaSparkDBSCAN.spatial.{Box, Point, PointSortKey}
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.spark.rdd.RDD
import ScalaSparkDBSCAN._
import ScalaSparkDBSCAN.dbscan.RawDataSet

import scala.Some

private [ScalaSparkDBSCAN] class PointIndexer (val numberOfPartitions: Int, val currentPartition: Int) extends Serializable {

  val multiplier = computeMultiplier (numberOfPartitions)
  var currentIndex = 0

  def getNextIndex = {
    currentIndex += 1
    currentIndex * multiplier + currentPartition
  }

  def computeMultiplier (numberOfPartitions: Int) = {
    val numberOfDigits = Math.floor (java.lang.Math.log10 (numberOfPartitions)) + 1

    Math.round (Math.pow (10, numberOfDigits))
  }

}

private [ScalaSparkDBSCAN] object PointIndexer extends Serializable {

  def addMetadataToPoints (
      data: RawDataSet,
      boxes: Broadcast[Iterable[Box]],
      dimensions: Broadcast[Int],
      distanceMeasure: DistanceMeasure): RDD[(PointSortKey, Point)] = {

    val numPartitions = data.partitions.length
    val origin = new Point (Array.fill (dimensions.value)(0.0))

    data.mapPartitionsWithIndex( (partitionIndex, points) => {

      val pointIndexer = new PointIndexer (numPartitions, partitionIndex)

      points.map (pt => {

        val pointIndex = pointIndexer.getNextIndex
        val box = boxes.value.find( _.isPointWithin(pt) )
        val distanceFromOrigin = distanceMeasure.compute(pt.coordinates.toArray, origin.coordinates.toArray)
        val boxId = box match {
          case existingBox: Some[Box] => existingBox.get.boxId
          case _ => 0 // throw an exception?
        }

        val newPoint = new Point (pt.coordinates, pointIndex, boxId, distanceFromOrigin,
            pt.precomputedNumberOfNeighbors, pt.clusterId)

        (new PointSortKey (newPoint), newPoint)
      })
    })

  }
}
