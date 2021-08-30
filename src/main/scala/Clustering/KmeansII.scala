/*
Scalable K-Means++ is a different implementation of the K-means algorithm where changes the initialization phase,
that is made more efficient, the initial centroids are not chosen at random.

This class uses the Spark API MLlib to implement the algorithm.

This is an iterative algorithm that will make multiple passes over the data, so any RDDs given to it should be
cached by the user.

Useful resources:
  - https://spark.apache.org/docs/latest/ml-clustering.html
  - https://spark.apache.org/docs/latest/mllib-clustering.html#k-means (better)
  - https://hub.packtpub.com/implementing-apache-spark-k-means-clustering-method-on-digital-breath-test-data-for-road-safety/

@Author: Daniele Filippini
 */

package Clustering

import DataPreprocessing.postprocessing.computeSummary
import Utils.Plot.{saveElbowLinePlot, savePairPlot}
import Utils.Const._
import Utils.common.{getRunningTime, writeListToTxt, writeResToCSV}
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.mllib.clustering.KMeans.K_MEANS_PARALLEL
import org.apache.spark.mllib.clustering.KMeans._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.SparkSession
import Utils.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.functions._
import com.cibo.evilplot.colors.Color
import com.cibo.evilplot.plot.components.Position


object KmeansII {

  val maxIterations = 250   // is the maximum number of iterations to run.

  /**
   * run the algorithm several times and compute the mean time needed to get the results. This is done for
   * a range of possible number of clusters.
   * @param VectorData input data
   * @param epsilon stopping threshold
   */
  private [KmeansII] def test_computational_time(VectorData: RDD[org.apache.spark.mllib.linalg.Vector], epsilon: Double): Unit = {
    val tmp_res = for {
      num_centroids <- 2 to 10
      avg_list = for {
        i <- 0 until 10
      } yield getRunningTime(run(VectorData, num_centroids, epsilon, K_MEANS_PARALLEL))
      mean = avg_list.sum / avg_list.length
    } yield (num_centroids, mean).toString()

    println("Results:")
    tmp_res.foreach(println)
    writeListToTxt("kmeansII_running_times_standard.txt", tmp_res)
  }

  /**
   * Compute the elbow graph by calculating the WSSSE for each number of clusters
   * @param start min number of clusters
   * @param end max number of clusters
   * @param vectorData RDD[points]
   * @param epsilon threshold to stop the computation
   * @param initialization_type this could be RANDOM or K_MEANS_PARALLEL
   * @param filename png file in which the result will be saved
   */
  def computeElbow(start: Int, end: Int, vectorData: RDD[org.apache.spark.mllib.linalg.Vector], epsilon: Double,
                   initialization_type: String, filename: String = "kmeansII_elbow_plot.png") = {
    val clusters_range = start to end
    val wss_list = for{
      num_centroids <- clusters_range
      // compute kmeans
      wss = time(runAndGetWSS(vectorData, num_centroids, epsilon, initialization_type))
    } yield wss

    saveElbowLinePlot(wss_list, clusters_range, filename)
  }

  /**
   * Run the algorithm to know the WSSSE for a specific number of clusters
   * @param VectorData RDD[points]
   * @param numClusters
   * @param epsilon stopping threshold
   * @param initializationMode this could be RADOM or K_MEANS_PARALLEL
   * @return WSSSE
   */
  def runAndGetWSS(VectorData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, epsilon: Double,
                   initializationMode: String) = {
    val kMeans = new KMeans()
    kMeans.setK( numClusters )
    kMeans.setMaxIterations( maxIterations )
    kMeans.setInitializationMode( initializationMode )
    kMeans.setEpsilon( epsilon )
    kMeans.setSeed(4)

    val kMeansModel = kMeans.run( VectorData )

    // (4) Evaluate clustering by computing Within Set Sum of Squared Errors
    val kMeansCost_WSS = kMeansModel.computeCost( VectorData ) // WSS = Within Set Sum of Squared Error

    kMeansCost_WSS
  }

  // Run the MLlib version of the kmeans algorithm
  def run(VectorData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, epsilon: Double,
          initializationMode: String) = {
    // (2) A KMeans object is initialized and the parameters are set to define the number of clusters and
    // the maximum number of iterations to determine them
    val kMeans = new KMeans()
    kMeans.setK( numClusters )
    kMeans.setMaxIterations( maxIterations )
    kMeans.setInitializationMode( initializationMode )
    kMeans.setEpsilon( epsilon )
    kMeans.setSeed(4)

    // (3) Train the model to get the clusters, there are two functions available
    // run(): Train a K-means model on the given set of points; data should be cached for high performance,
    // because this is an iterative algorithm.
    // train(): Trains a k-means model using the given set of parameters.
    // documentation kmenasmodel class:
    // https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/clustering/KMeansModel.html
    val kMeansModel = kMeans.run( VectorData )


    // (4) Evaluate clustering by computing Within Set Sum of Squared Errors
    val kMeansCost_WSS = kMeansModel.computeCost( VectorData ) // WSS = Within Set Sum of Squared Error

    println( "Input data rows : " + VectorData.count() )
    println( "K-Means Cost  : " + kMeansCost_WSS )
    println( "Training cost: " + kMeansModel.trainingCost)
    val centroids = kMeansModel.clusterCenters.map(vec => vec.toArray.toVector)
    println("Cluster centers:")
    kMeansModel.clusterCenters.foreach{ println }

//    val points = VectorData.collect().map(vec => vec.toArray.toVector)
//    val clustered_points = kMeansModel.predict( VectorData ).collect() zip points

    val clustered_points = (kMeansModel.predict( VectorData ) zip VectorData).map(x => (x._1, x._2.toArray.toVector))

    // (5) Save model
    //kMeansModel.save(spark.sparkContext, base_path) // This saves: - human-readable (JSON) model metadata to path/metadata/ - Parquet formatted data to path/data/
    // load model
    //val sameModel = KMeansModel.load(spark.sparkContext, base_path)

    (centroids, clustered_points)
  }

  /**
   * load data of given a specific dataset path and returns an RDD of that data
   * @param dataset_path filepath
   * @return a pair (column, RDD) where column are the columns of the dataset and RDD contains the points
   */
  def loadData(dataset_path: String, spark: SparkSession) = {
    println("Loading data.....")

    // (1) the CSV data is loaded from the data file and split by comma characters into the VectorData variable
    val data = spark.sparkContext.textFile(dataset_path)
    var csvData: RDD[String] = data
    val hasHeader = true
    var columns: Array[String] = Array()
    if(hasHeader){
      val header = data.first() // extract the header
      columns = header.split(",")
      csvData = data.filter(row => row != header)  // filter out reader
    }

    val VectorData = csvData.map {
      csvLine => Vectors.dense( csvLine.split(',').map(_.toDouble) )
    }.cache()

    println("Data loaded.")
    (columns, VectorData)
  }

  // Run the K-Means mllib standard version
  def kmeans(dataset_path: String, epsilon: Double, numK: Int, plot_data: Boolean = false, spark: SparkSession) = {
    println("Running kmeans clustering.....")
    // (A) load data
    val (columns, data) = loadData(dataset_path, spark)

    // (B) run kmeans algorithm
    val (centroids, clustered_points_rdd) = time(run(data, numK, epsilon, RANDOM))
    val clustered_points = clustered_points_rdd.collect()

    // (C) save the results - //Vector("Recency", "Frequency", "MonetaryValue")
//    writeResToCSV("kmeans_clustering_results.csv", clustered_points, columns.toVector) //run in local mode
    writeResToCSV_AWS_S3("kmeans_clustering_results.csv", clustered_points, columns.toVector) //run on cluster
    if(plot_data)
      savePairPlot(clustered_points, columns, img_pkg_path+"/kmeans_pairplot.png")

    println("Computing results......")
    computeSummary(columns, clustered_points_rdd, spark)
  }

  // Run K-Means with a different initialization procedure that can be more efficient and can give better results
  def Scalable_KMeans(dataset_path: String, epsilon: Double, numK: Int, plot_data: Boolean = false, spark: SparkSession) = {
    println("Running scalable kmeans......")
    // (A) load data
    val (columns, data) = loadData(dataset_path, spark)

    // (B) run kmeans algorithm
    val (centroids, clustered_points_rdd) = time(run(data, numK, epsilon, K_MEANS_PARALLEL))
    val clustered_points = clustered_points_rdd.collect()

    // (C) save the results
//    writeResToCSV("kmeans_scalable_clustering_results.csv", clustered_points, columns.toVector) //run in local mode
    writeResToCSV_AWS_S3("kmeans_scalable_clustering_results.csv", clustered_points, columns.toVector) //run on cluster
    if(plot_data)
      savePairPlot(clustered_points, columns, img_pkg_path+"/kmeans_scalable_pairplot.png") //Vector("Recency", "Frequency", "MonetaryValue")

    println("Computing results......")
    computeSummary(columns, clustered_points_rdd, spark)
  }


  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkSession.builder()
      .appName("KMeans||")
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // using spark session we do not need the spark context anymore
    //    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


//    // (A) Run the model
////    println("EXECUTION OF THE KMEANS|| MODEL (MLlib version)")
////    time(run(VectorData, 4))
//
//    // (B) Elbow method to know the best number of clusters
//    val epsilon = 1.5
//    computeElbow(2, 10, VectorData, epsilon, K_MEANS_PARALLEL)

//    test_computational_time(VectorData)

    println("Run Kmeans|| example.......")
    Scalable_KMeans(onlineretail_file, 0.0001, 5, false, spark)

  }
}
