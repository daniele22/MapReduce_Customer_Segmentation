package ScalaSparkDBSCAN.exploratoryAnalysis

import ScalaSparkDBSCAN.util.commandLine._
import org.apache.spark.SparkContext
import ScalaSparkDBSCAN.util.io.IOHelper
import ScalaSparkDBSCAN.DbscanSettings
import ScalaSparkDBSCAN.spatial.rdd.{PointsPartitionedByBoxesRDD, PartitioningSettings}
import ScalaSparkDBSCAN.spatial.{DistanceCalculation, Point, PointSortKey, DistanceAnalyzer}
import org.apache.commons.math3.ml.distance.DistanceMeasure
import ScalaSparkDBSCAN.util.debug.Clock
import ScalaSparkDBSCAN.dbscan.RawDataSet

/** A driver program which estimates distances to nearest neighbor of each point
 *
 */
object DistanceToNearestNeighborDriver extends DistanceCalculation with Serializable {

  private [ScalaSparkDBSCAN] class Args extends CommonArgs with NumberOfBucketsArg with NumberOfPointsInPartitionArg

  private [ScalaSparkDBSCAN] class ArgsParser
    extends CommonArgsParser (new Args (), "DistancesToNearestNeighborDriver")
    with NumberOfBucketsArgParsing [Args]
    with NumberOfPointsInPartitionParsing [Args]

  def main (args: Array[String]) {
    val argsParser = new ArgsParser()

    if (argsParser.parse(args, Config()).isDefined) {
      val clock = new Clock()

      val sc = new SparkContext(argsParser.args.masterUrl,
        "Estimation of distance to the nearest neighbor",
        jars = Array(argsParser.args.jar))

      val (columns, data) = IOHelper.readDataset(sc, argsParser.args.inputPath, hasHeader = false)
      val settings = new DbscanSettings().withDistanceMeasure(argsParser.args.distanceMeasure)
      val partitioningSettings = new PartitioningSettings(numberOfPointsInBox = argsParser.args.numberOfPoints)
      
      val histogram = createNearestNeighborHistogram(data: RawDataSet, settings, partitioningSettings)
      
      val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

      IOHelper.saveTriples(sc.parallelize(triples), argsParser.args.outputPath)

      clock.logTimeSinceStart("Estimation of distance to the nearest neighbor")
    }
  }

  def run(data: RawDataSet, settings: DbscanSettings): Seq[(Double, Double, Long)] = {
    val partitioningSettings = new PartitioningSettings(numberOfPointsInBox =
      PartitioningSettings.DefaultNumberOfPointsInBox)
    val histogram = createNearestNeighborHistogram(data: RawDataSet, settings, partitioningSettings)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)
    triples
  }

  /**
   * This method allows for the histogram to be created and used within an application.
   */
  def createNearestNeighborHistogram(
    data: RawDataSet,
    settings: DbscanSettings = new DbscanSettings(),
    partitioningSettings: PartitioningSettings = new PartitioningSettings()) = {
    
    val partitionedData = PointsPartitionedByBoxesRDD(data, partitioningSettings)
    
    val pointIdsWithDistances = partitionedData.mapPartitions {
      it =>
        {
          calculateDistancesToNearestNeighbors(it, settings.distanceMeasure)
        }
    }

    ExploratoryAnalysisHelper.calculateHistogram(pointIdsWithDistances)
  }

  private[ScalaSparkDBSCAN] def calculateDistancesToNearestNeighbors(
    it: Iterator[(PointSortKey, Point)],
    distanceMeasure: DistanceMeasure) = {

    val sortedPoints = it
      .map ( x => new PointWithDistanceToNearestNeighbor(x._2) )
      .toArray
      .sortBy( _.distanceFromOrigin )

    var previousPoints: List[PointWithDistanceToNearestNeighbor] = Nil

    for (currentPoint <- sortedPoints) {

      for (p <- previousPoints) {
        val d = calculateDistance(currentPoint, p)(distanceMeasure)

        if (p.distanceToNearestNeighbor > d) {
          p.distanceToNearestNeighbor = d
        }

        if (currentPoint.distanceToNearestNeighbor > d) {
          currentPoint.distanceToNearestNeighbor = d
        }
      }

      previousPoints = currentPoint :: previousPoints.filter {
        p => {
          val d = currentPoint.distanceFromOrigin - p.distanceFromOrigin
          p.distanceToNearestNeighbor >= d
        }
      }
    }

    sortedPoints.filter( _.distanceToNearestNeighbor < Double.MaxValue).map ( x => (x.pointId, x.distanceToNearestNeighbor)).iterator
  }
}
