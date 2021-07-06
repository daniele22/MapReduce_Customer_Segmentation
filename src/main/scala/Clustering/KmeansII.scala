/*
This is an iterative algorithm that will make multiple passes over the data, so any RDDs given to it should be cached by the user.

Useful resources:
  - https://spark.apache.org/docs/latest/ml-clustering.html
  - https://spark.apache.org/docs/latest/mllib-clustering.html#k-means (better)
  - https://hub.packtpub.com/implementing-apache-spark-k-means-clustering-method-on-digital-breath-test-data-for-road-safety/

@Author: Daniele Filippini
 */

package Clustering

import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.mllib.clustering.KMeans.K_MEANS_PARALLEL
import org.apache.spark.mllib.clustering.KMeans._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.SparkSession
import myparallel.primitives._
import org.apache.spark.rdd.RDD


object KmeansII {

  val base_path = "C:/Users/hp/IdeaProjects/MapReduce_Customer_Segmentation/src/main/scala/Clustering"
  //val inference_filename: String = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/dataset/Online_Retail_II/customer_data.csv"
  val inference_filename: String = base_path+"/test_points.csv"


  // Parameters of the model
  val numClusters = 4
  val maxIterations = 50
  // Some default values are defined for the initialization mode, number of runs, and Epsilon, which we needed
  // for the K-Means call but did not vary for the processing.
  // Set the initialization algorithm. This can be either "random" to choose random points as initial cluster centers, or "k-means||" to use a parallel variant of k-means++ (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
  val initializationMode: String = K_MEANS_PARALLEL  // this is the new type of initialization that should be more efficient
//  val initializationMode: String = RANDOM // with random initialization mode we will get same resul as the original lloyd's algorithm
  val numEpsilon = 1e-4

  def main(args: Array[String]): Unit = {

    // start spark session, it contains the spark context
    val spark : SparkSession = SparkSession.builder()
      .appName("KMeans")
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // using spark session we do not need the spark context anymore
    //    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // (1) the CSV data is loaded from the data file and split by comma characters into the VectorData variable
    val data = spark.sparkContext.textFile(inference_filename)
    var csvData: RDD[String] = data
    val hasHeader = true
    if(hasHeader){
      val header = data.first() // extract the header
      csvData = data.filter(row => row != header)  // filter out reader
    }

    val VectorData = csvData.map {
      csvLine => Vectors.dense( csvLine.split(',').map(_.toDouble))
    }.cache()

    def run(): Unit = {
      // (2) A KMeans object is initialized and the parameters are set to define the number of clusters and
      // the maximum number of iterations to determine them
      val kMeans = new KMeans()
      kMeans.setK( numClusters )
      kMeans.setMaxIterations( maxIterations )
      kMeans.setInitializationMode( initializationMode )
      kMeans.setEpsilon( numEpsilon )
      kMeans.setSeed(42)

      // (3) Train the model to get the clusters, there are two functions available
      // run(): Train a K-means model on the given set of points; data should be cached for high performance, because this is an iterative algorithm.
      // train(): Trains a k-means model using the given set of parameters.
      // documentation kmenasmodel class: https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/clustering/KMeansModel.html
      val kMeansModel = kMeans.run( VectorData )

      // (4) Evaluate clustering by computing Within Set Sum of Squared Errors
//      val kMeansCost_WSS = kMeansModel.computeCost( VectorData ) // WSS = Within Set Sum of Squared Error
//      println( "Input data rows : " + VectorData.count() )
//      println( "K-Means Cost  : " + kMeansCost_WSS )
//      println( "Training cost: " + kMeansModel.trainingCost)
      kMeansModel.clusterCenters.foreach{ println }

      // (5) Save model
      //kMeansModel.save(spark.sparkContext, base_path) // This saves: - human-readable (JSON) model metadata to path/metadata/ - Parquet formatted data to path/data/
      // load model
      //val sameModel = KMeansModel.load(spark.sparkContext, base_path)
    }

    // Run the model
    println("EXECUTION OF THE KMEANS|| MODEL (MLlib version)")
    time(run())

  }
}
