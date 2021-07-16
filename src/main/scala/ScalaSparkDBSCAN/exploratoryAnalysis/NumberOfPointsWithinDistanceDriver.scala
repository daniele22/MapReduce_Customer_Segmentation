package ScalaSparkDBSCAN.exploratoryAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import ScalaSparkDBSCAN.util.io.IOHelper
import ScalaSparkDBSCAN.DbscanSettings
import ExploratoryAnalysisHelper._
import ScalaSparkDBSCAN.util.commandLine._
import ScalaSparkDBSCAN.spatial.DistanceAnalyzer
import ScalaSparkDBSCAN.spatial.rdd.{PartitioningSettings, PointsPartitionedByBoxesRDD}
import ScalaSparkDBSCAN.util.debug.Clock
import ScalaSparkDBSCAN.dbscan.RawDataSet

/** A driver program which calculates number of point's neighbors within specified distance
  * and generates a histogram of distribution of this number across the data set
  *
  */
object NumberOfPointsWithinDistanceDriver extends Serializable {

  private [ScalaSparkDBSCAN] class Args extends CommonArgs with NumberOfBucketsArg with EpsArg with NumberOfPointsInPartitionArg

  private [ScalaSparkDBSCAN] class ArgsParser
    extends CommonArgsParser (new Args (), "NumberOfPointsWithinDistanceDriver")
    with NumberOfBucketsArgParsing [Args]
    with EpsArgParsing[Args]
    with NumberOfPointsInPartitionParsing[Args]


  def main (args: Array[String]) {
    val argsParser = new ArgsParser()

    if (argsParser.parse(args, Config()).isDefined) {
      val clock = new Clock ()
      val distance = argsParser.args.eps

      val sc = new SparkContext(argsParser.args.masterUrl,
        "Histogram of number of points within " + distance + " from each point",
        jars = Array(argsParser.args.jar))

      val data = IOHelper.readDataset(sc, argsParser.args.inputPath, false)

      val settings = new DbscanSettings()
        .withEpsilon(distance)
        .withDistanceMeasure(argsParser.args.distanceMeasure)

      val partitioningSettings = new PartitioningSettings (numberOfPointsInBox = argsParser.args.numberOfPoints)
      
      val histogram = createNumberOfPointsWithinDistanceHistogram(data, settings, partitioningSettings, argsParser.args.numberOfBuckets)

      val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

      IOHelper.saveTriples(sc.parallelize(triples), argsParser.args.outputPath)

      clock.logTimeSinceStart("Calculation of number of points within " + distance)
    }
  }

  def run(data: RawDataSet, settings: DbscanSettings): Seq[(Double, Double, Long)] = {
    val partitioningSettings = new PartitioningSettings(numberOfPointsInBox =
      PartitioningSettings.DefaultNumberOfPointsInBox)
    val histogram = createNumberOfPointsWithinDistanceHistogram(data, settings, partitioningSettings)
      //argsParser.args.numberOfBuckets) //TODO check!

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)
    triples
  }

  /**
   * This method allows for the histogram to be created and used programmatically.
   * 
   * Requires DbscanSettings to be set, because it must include value for epsilon.
   */
  def createNumberOfPointsWithinDistanceHistogram(
    data: RawDataSet,
    settings: DbscanSettings,
    partitioningSettings: PartitioningSettings = new PartitioningSettings(),
    numberOfBuckets: Int = -1): (Array[Double], Array[Long]) = {
    
    val partitionedData = PointsPartitionedByBoxesRDD (data, partitioningSettings, settings)
    val distanceAnalyzer = new DistanceAnalyzer(settings)
    val closePoints = distanceAnalyzer.countClosePoints(partitionedData)

    val countsOfPointsWithNeighbors = closePoints
      .map(x => (x._1.pointId, x._2))
      .foldByKey (1L)(_+_)
      .cache()

    val indexedPoints = PointsPartitionedByBoxesRDD.extractPointIdsAndCoordinates (partitionedData)

    val countsOfPointsWithoutNeighbors = indexedPoints
      .keys
      .subtract(countsOfPointsWithNeighbors.keys)
      .map((_, 0L))
    
    val allCounts = countsOfPointsWithNeighbors union countsOfPointsWithoutNeighbors
    
    allCounts.persist()
    
    val histogram = if(numberOfBuckets < 1) ExploratoryAnalysisHelper.calculateHistogram(allCounts) else ExploratoryAnalysisHelper.calculateHistogram(allCounts, numberOfBuckets)
    
    allCounts.unpersist()
    
    histogram
  }

}
