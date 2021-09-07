/*
This class is used to run the DBSCAN algorithm on a distributed cluster using spark.

Useful resources:
https://towardsdatascience.com/explaining-dbscan-clustering-18eaf5c83b31

@author Daniele Filippini
 */

package Clustering


import DataPreprocessing.postprocessing.computeSummary
import ScalaSparkDBSCAN._
import ScalaSparkDBSCAN.dbscan.ClusterId
import ScalaSparkDBSCAN.spatial.Point
import ScalaSparkDBSCAN.util.io.IOHelper
import Utils.Plot
import Utils.Plot.savePairPlot
import Utils.common.{time, writeListToTxt, writeResToCSV, writeResToCSV_AWS_S3}
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.sql.SparkSession
import ScalaSparkDBSCAN.exploratoryAnalysis.DistanceToNearestNeighborDriver
import org.apache.spark.rdd.RDD
import ScalaSparkDBSCAN.exploratoryAnalysis.NumberOfPointsWithinDistanceDriver
import Utils.Const._


object DBSCAN_Distributed extends java.io.Serializable{

  /**
   * Count the number of clusters defined by the dbscan algorithm
   * @param model dbscan
   * @return number of clusters
   */
  private [DBSCAN_Distributed] def numberOfClusters(model: DbscanModel): Int = {
    val clusterIdsRdd: RDD[ClusterId] = model.allPoints.map(_.clusterId) // get the clusterId assigned to each point
      .distinct()  // get a new RDD containing the distinct elements in this RDD
      .filter(clusterId => clusterId != DbscanModel.NoisePoint) // remove the clusterId equal to that assigned to noise point if present
      .cache()
    // count is an action, i.e. an eager operation that returns a single number.
    // The previous operations were transformations, they transformed an RDD into another lazily.
    // REMEMBER: In effect the transformations were not actually performed, just queued up. When we call count(),
    // we force all the previous lazy operations to be performed.
    // if we call count() twice, all this will happen twice.
    // After the count is returned, all the data is discarded!
    // To avoid this, cache() is added on the RDD. In this case the RDD will have to be stored in memory (or disk).
    clusterIdsRdd.count().toInt
  }

  /**
   * For each cluster defined by the model counts the number of points in it.
   * @param model dbscan
   * @return a pair with cluster id and the relative number of points in it
   */
  private [DBSCAN_Distributed] def countPointsPerCluster(model: DbscanModel): RDD[(ClusterId, Int)] = {
    val clusteredPoints = model.clusteredPoints
    //clusteredPoints.map(point => (point.clusterId, 1)).countByKey()
    clusteredPoints.map(point => (point.clusterId, 1)).reduceByKey(_+_)  // points per cluster
  }


  /**
   * run the script to compute the distance of each point o the nearest neighbour.
   * @param filepath input data
   * @param hasHeader boolean that specify if the input file has or not the header
   * @return list of tuples with the number of points per distance range
   */
  def getDistanceToNearestNeighbor(filepath: String, hasHeader: Boolean, spark: SparkSession) = {
    val epsilon = 1.0 // not used to compute the results
    val minPts = 5 // not used to compute the results
    println("Read csv file")
    // Read csv file with the IOHelper class
    val (columns, data) = IOHelper.readDataset(spark.sparkContext, filepath, hasHeader)

    println("Set dbscan settings")
    // Specify parameters of the DBSCAN algorithm using SparkSettings class
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)

    val triple1 = DistanceToNearestNeighborDriver.run(data, clusteringSettings)
    println("Triple 1 - Distance To Nearest Neighbor:")
    println(triple1)
    writeListToTxt("dbscan_DistanceToNearestNeighbor_tuples.txt", triple1.map(x => "("+x._1+", "+x._2+"),"+x._3))

    val X = triple1.map(element => "(" + element._1.toString + " - " + element._2.toString + ")")
    val Y = triple1.map(_._3)
    Plot.saveBarChart(Y.map(_.toDouble), X, img_pkg_path+"/dbscan_DistanceToNearestNeighbor1.png")

    triple1
  }

  /**
   * run the script to get the number of points within a distance per point.
   * @param filepath input data
   * @param hasHeader boolean param that specify if the input file has or not the header
   * @param epsilon distance value
   * @return list of tuples with the results per range
   */
  def getNumberOfPointsWithinDistance(filepath: String, hasHeader: Boolean, epsilon: Double, spark: SparkSession) = {
    val minPts = 5 // not used to compute the results
    println("Read csv file")
    // Read csv file with the IOHelper class
    val (columns, data) = IOHelper.readDataset(spark.sparkContext, filepath, hasHeader)

    println("Set dbscan settings")
    // Specify parameters of the DBSCAN algorithm using SparkSettings class
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)

    val triple2 = NumberOfPointsWithinDistanceDriver.run(data, clusteringSettings)
    println("Triple 2 - Number Of Points Within Distance:")
    println(triple2)
    writeListToTxt("dbscan_NumberOfPointsWithinDistance_tuples.txt", triple2.map(x => "("+x._1+", "+x._2+"),"+x._3))

    val X2 = triple2.map(element => "(" + element._1.toString + " - " + element._2.toString + ")")
    val Y2 = triple2.map(_._3)
    Plot.saveBarChart(Y2.map(_.toDouble), X2, img_pkg_path+"/dbscan_NumberOfPointsWithinDistance1.png")

    triple2
  }


  /**
   * Run the dbscan algorithm on a distributed cluster using spark.
   * @param dataset_path filepath
   * @param epsilon value for the distance
   * @param minPts value for the min number of points in a cluster
   * @param plot_data boolean value that defines if make or not the pair plot with all the points of each cluster
   */
  def dbscan(dataset_path: String, epsilon: Double, minPts: Int, plot_data: Boolean = false, spark: SparkSession) = {
    println("Read csv file")
    // Read csv file with the IOHelper class
    val (columns, data) = IOHelper.readDataset(spark.sparkContext, dataset_path, hasHeader = true)

    println("Set dbscan settings")
    // Specify parameters of the DBSCAN algorithm using SparkSettings class
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)

    // Run clustering algorithm
    println("Running dbscan....")
    val model = time(Dbscan.train(data, clusteringSettings))

    println("Number of clusters identified by the model: " + numberOfClusters(model))

    println("Number of clustered points: "+ model.clusteredPoints.count().toInt)

    val pointsPerCluster = countPointsPerCluster(model)
    println("Number of points per cluster:")
    pointsPerCluster.foreach(println)

    val noisePoints = model.noisePoints
    val numOfNoisePoints = noisePoints.count().toInt
    println("Number of noise points: " + numOfNoisePoints)

//    println("Cluster ids")
//    model.allPoints.foreach(x => println(x.clusterId))

    val clustered_points_rdd = model.allPoints.map(pt => (pt.clusterId.toInt, pt.coordinates.toVector))//.collect()
    val clustered_points = clustered_points_rdd.collect()

//    writeResToCSV("dbscan_clustering_results.csv", clustered_points, columns.toVector) //run in local mode
    writeResToCSV_AWS_S3("dbscan_clustering_results.csv", clustered_points, columns.toVector) //run on cluster
    if(plot_data)
      savePairPlot(clustered_points, columns, img_pkg_path+"/dbscan_pairplot.png")

    computeSummary(columns, clustered_points_rdd, spark)
  }

  def main(args: Array[String]): Unit = {

    // start spark session, it contains the spark context
    val spark : SparkSession = SparkSession.builder()
      .appName("DBSCAN")
      .master("local[*]")
      .getOrCreate()

//    val minPts = 6
//    val epsilon = 3

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // run dbscan example
    println("Run dbscan example.......")
    dbscan(instacart_file, 0.3, 50, false, spark)
  }

}
