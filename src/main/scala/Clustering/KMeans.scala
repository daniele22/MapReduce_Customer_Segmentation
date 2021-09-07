/*
Main Steps of KMEANS algorithm:
  - Choose K random geo-located points as starting centers
  - Find all points closest to each center
  - Find the new center of each cluster
  - Loop until the total distance between one iteration's points and the next is less than the convergence distance specified

This is a general script that can work with points of different sizes, so in different vectorial spaces.
The points are represented with an Array of Double, where each element is one of the axes of the vectorial space.

With this class we can also generate a synthetic file, with an arbitrary number of features, that could be used to
test the different clustering algorithms

Useful resources: https://gist.github.com/umbertogriffo/b599aa9b9a156bb1e8775c8cdbfb688a

@Author: Daniele Filippini
 */
package Clustering

import java.io._

import DataPreprocessing.postprocessing.computeSummary
import Utils.Const._
import Utils.Plot.{saveElbowLinePlot, savePairPlot}
import Utils.common.{getRunningTime, time, writeListToTxt, writeResToCSV}
import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.json4s.scalap.scalasig.ClassFileParser.header

import scala.util.Random
import Utils.common._
import org.apache.log4j.{Logger => mylogger}
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec



//spark session with schema
import org.apache.spark.sql.types._


object KMeans extends java.io.Serializable{

  val maxIterations = 250

  /**
   * Compute the euclidean distance between two points. The points are represented through
   * vectors, each element of the vector represent a specific dimension
   * @param p1 vector of coordinates of the first point
   * @param p2 vector of coordinates of the second point
   * @return
   */
  private [KMeans] def euclideanDistance(p1: Vector[Double], p2: Vector[Double]): Double = {
    math.sqrt((p1 zip p2).map{
      case (elem1, elem2) => math.pow(elem2 - elem1, 2)  // without case it does not work because the compiler is not able to type the variables elem1 and elem2
      case _ => throw new Exception("Error in euclidean distance calculation.")
    }.sum)
  }

  /**
   * Compute the squared distance between two points. The points are represented through
   * vectors, each element of the vector represent a specific dimension
   * @param p1 vector of coordinates of the first point
   * @param p2 vector of coordinates of the second point
   * @return
   */
  private [KMeans] def squaredDistance(p1: Vector[Double], p2: Vector[Double]): Double = {
    (p1 zip p2).map{
      case (elem1, elem2) => math.pow(elem2 - elem1, 2)  // without case it does not work because the compiler is not able to type the variables elem1 and elem2
      case _ => throw new Exception("Error in squared distance calculation.")
    }.sum
  }

  /**
   * Find the nearest centroid of a specific point.
   * @param p vector of coordinates of a point
   * @param centroids list of all centroids coordinates
   * @return the index of the nearest centroid
   */
  private [KMeans] def findClosest(p: Vector[Double],
                  centroids: Array[(Int, Vector[Double])]): Int =
    centroids.map(c => (c._1, euclideanDistance(c._2, p))) // calculate the distance between p and each one of the centroids
      .minBy(_._2)._1 // take the centroid with the smallest euclidean distance and return its number

  /**
   * Compute the weighted mean of a single feature of two different points
   * @param p1_attribute an attribute of the first point (dimension of the vectorial space)
   * @param p1_weight weight associated to the first point
   * @param p2_attribute the same attribute of the second point
   * @param p2_weight weight associated to the second point
   * @return the weighted mean of that specific attribute
   */
  private [KMeans] def weightedMean(p1_attribute: Double, p1_weight: Double, p2_attribute: Double, p2_weight: Double): Double = {
    1.0/(1.0+p2_weight/p1_weight)*p1_attribute + 1.0/(1.0+p1_weight/p2_weight)*p2_attribute
  }


  /**
   * this function computes the weighted mean of two different points, at the beginning the weight associated
   * to each point will be equal to 1, and then at each iteration the weights of two points are added.
   * We need this approach to calculate the mean point in an efficient way in a distributed context,
   * we do not want a sequential implementation that is more slow.
   * @param p1 couple where the first element is the vector of coordinates of the point and the second is
   *           the weight associated to that specific point
   * @param p2 as p1 is a couple with vector and weight as member
   * @return a couple that contains the vector of coordinates of the mean point and the new weight that will be
   *         the sum of the weights of p1 and p2
   */
  private [KMeans] def weightedMeanPoint(p1: (Vector[Double], Double),
                        p2: (Vector[Double], Double)): (Vector[Double], Double) = {
    (
      // this is the first element of the couple, it's the mean point between p1 and p2
      (p1._1 zip p2._1).map{
        case (elem1, elem2) => weightedMean(elem1, p1._2, elem2, p2._2)  // without case it does not work beacuse the compiler is not able to type the variables elem1 and elem2
        case _ => throw new Exception("Error in euclidean distance calculation.")
      },
      // this is the second element of the couple and is the sum of the weights associated with the two points p1 and p2
      p1._2 + p2._2
    )
  }

  //
  /**
   * Compute the mean distance between the centroids before the update and the new centroids computed.
   * @param old_centroids list of the centroids at the previous iteration
   * @param new_centroids list of new centroids computed
   * @return
   */
  private [KMeans] def meanDistance(old_centroids: Array[(Int, Vector[Double])],
                   new_centroids: Array[(Int, Vector[Double])]): Double =
    ((old_centroids zip new_centroids)
      .map (c => euclideanDistance(c._1._2, c._2._2))
      .sum) / old_centroids.length

  /**
   * Print centroids with their id
   * @param centroids list of centroids
   */
  private [KMeans] def printCentroids(centroids: Array[(Int, Vector[Double])]): Unit = {
    for ((cen, elementsList) <- centroids){
      print("Centroid # " + cen + ": (")
      for ((element, index) <- elementsList.zipWithIndex) {
        if (index == (elementsList.length-1)) print(element)
        else print(element + ",")
      }
      println(")")
    }
  }

  /**
   * Compute the WSSSE (Within set sum of squared distance), this is a measure that is used to evaluate the quality
   * of the clusters. It mesures the dispersion of the points w.r.t. the cluster center
   * @param centroids list of centroids
   * @param clustered_points list of points with the cluster assigned to them
   * @return WSSSE
   */
  private [KMeans] def computeWSS(centroids: Array[Vector[Double]],
                                  clustered_points: Array[(Int, Vector[Double])],
                                  spark: SparkSession) = {
    // create rdd
    val rdd_points = spark.sparkContext.parallelize(clustered_points)
    // compute WSSSE
    // pair._1 will be the index of the centroid, we take it from the list and calculate the squaredDistance
    // between the point and the centroid.
    rdd_points.map(pair => squaredDistance(centroids(pair._1), pair._2)).sum()
  }

  /**
   * Take an index and extract the coordinate of that centroid from a list.
   * This is useful when the list of centroids is not sorted
   * @param index int id of the centroid
   * @param centroids list of centroids with their index
   * @return Centroid rapresented by a vector of coordinates
   */
  private [KMeans] def getCentroid(index: Int, centroids: Array[(Int, Vector[Double])]): Vector[Double] = {
    // the key is the index of the cluster, the value are the point coordinates
    val centroid = centroids.find(centroid => centroid._1 == index)
    if(centroid.isEmpty) throw new Exception("Error in finding centroid with id: "+index)
    else centroid.get._2
  }

  /**
   * Runs 10 times the same algorithm and take the average time.
   * This is done fore different number of clusters (range 2 - 10) and the results are saved on a txt file
   * @param input_data_points points to be clusterd
   * @param epsilon threshold value to stop the computation
   */
  private [KMeans] def test_computational_time(input_data_points: RDD[Vector[Double]], epsilon: Double): Unit = {
    val tmp_res = for {
      num_centroids <- 2 to 10
      avg_list = for {
        i <- 0 until 10
      } yield getRunningTime(run_kmeans_reducebykey(input_data_points, num_centroids, epsilon))
      mean = avg_list.sum / avg_list.length
    } yield (num_centroids, mean).toString()

    println("Results:")
    tmp_res.foreach(println)
    writeListToTxt("kmeans_running_times_reducebykey_prova.txt", tmp_res)
  }

  /**
   * Compute the elbow graph by calculating the WSSSE for each number of clusters
   * @param start min number of clusters
   * @param end max number of clusters
   * @param input_data_points RDD[points]
   * @param epsilon threshold to stop the computation
   * @param f kmeans type to execute, e.g. run_reducebykey, run_groupby,...
   * @param filename png file in which the result will be saved
   */
  def computeElbow(start: Int, end: Int, input_data_points: RDD[Vector[Double]], epsilon: Double,
                   f: (RDD[Vector[Double]], Int, Double) => ( Array[(Int, Vector[Double])],  RDD[(Int, Vector[Double])]),
                   spark: SparkSession, filename: String = "kmeans_elbow_plot.png") = {
    // (E) Elbow method to know the best number of clusters
    val clusters_range = start to end
    val wss_list = for{
      num_centroids <- clusters_range
      // compute kmeans
      (centroids, clustered_points) = time(f(input_data_points, num_centroids, epsilon))
      // sort the list of centroids
      sorted_centroids = centroids.sortBy(centroid => centroid._1).map(centroid => centroid._2)
      // compute the "error" measure
      wss = computeWSS(sorted_centroids, clustered_points.collect(), spark)
    } yield wss

    saveElbowLinePlot(wss_list, clusters_range, filename)
  }

  def run_kmeans_grouby(input_data_points: RDD[Vector[Double]], numK: Int, epsilon: Double) = {

    // Initialize K random centroids, these will have the form of (centroid_number, coordinates: Array[Double])
    // so they are represented as couple, where the first element is an integer and represent the index of the centroid.
    // Fix a seed to ensure reproducibility!
    var centroids =
    ((0 until numK) zip
      input_data_points.takeSample(false, numK, 42))
      .toArray

    // print the initial centroids
    println("K Center points initialized :")
    printCentroids(centroids)

    //println("Run KMeans clustering algorithm - Version with GroupBy")
    //var mycentroids = centroids

    var finished = false
    var numIterations = 0

    // This is an iterative algorithm. At each step the result is improved until a point of convergence.
    do {
      /*
      we group the points based on the nearest centroid, we will obtain a list of point foreach centroid index
      es. 1, List(point1, point2, ...)
          2, List(...) ...
      Then we have to calculate the position of the new centroids, to do this we will create couples (point, 1.0) in the map phase.
      This is needed for the distributed calculation of the mean, 1.0 is the initial coefficient used to compute a weighted mean.
      After that in the reduce phase we will calculate the mean point between couple of points
      */
      val newCentroids = input_data_points
        .groupBy(p => (findClosest(p, centroids)))  // get the list of closed points foreach centroid
        // compute the new position of the centroids, the first map phase creates a couple and associates
        // the initial weight of 1.0, then a reduce phase is executed to compute the mean point
        .map(x => (x._1, (x._2.map((_, 1.0))).reduce(weightedMeanPoint)))
        .map(c => (c._1, c._2._1))  // create a couple with c_index and point coordinates, without the weight added before
        .collect()
      // compare the new centroids with the old ones
      // continue the loop until the change between two iterations is less than a specific threshold
      if (meanDistance(centroids, newCentroids) < epsilon || numIterations > maxIterations)
        finished = true
      else centroids = newCentroids
      numIterations = numIterations + 1
    } while(!finished)
    println("Final centroid points:")
    printCentroids(centroids)
    //mycentroids.map(println)
    println("Iterations = "+numIterations)

    val clustered_points = input_data_points.map(point => (findClosest(point, centroids), point))
    (centroids, clustered_points)
  }


  // In this second version reduceByKey is used instead of groupBy, this should make this second version more efficient.
  // With this second version also problems related to the memory consumption can be avoided, indeed
  // all the points are aggregated on a single node with groudBy and in the case of millions of data this could be
  // a problem. With reduceByKey we can avoid that phenomenon.
  def run_kmeans_reducebykey(input_data_points: RDD[Vector[Double]], numK: Int, epsilon: Double) = {

    // Initialize K random centroids, these will have the form of (centroid_number, coordinates: Array[Double])
    // so they are represented as couple, where the first element is an integer and represent the index of the centroid.
    // Fix a seed to ensure reproducibility!
    var centroids =
    ((0 until numK) zip
      input_data_points.takeSample(false, numK, 42)).
      toArray

    // print the initial centroids
    println("K Center points initialized :")
    printCentroids(centroids)

    //var mycentroids = centroids

    var finished = false
    var numIterations = 0

    // iterative process
    do {
      /*
      A map is used to create couple (nearest centroid, (point, 1.0))
      The mean point for each one of the centroids will be calculated on each node
      Then only those data (i.e. numK*numNodes) have to travel on the network towards the main node
      This means a minor use of the network and so less time needed.
       */
      val newCentroids = input_data_points
        .map(p => (findClosest(p, centroids), (p,1.0)))  // one is the initial weight
        .reduceByKey(weightedMeanPoint)
        .map(c => (c._1,c._2._1))
        .collect()
      // compare the new centroid with the old ones
      if (meanDistance(centroids,newCentroids) < epsilon || numIterations > maxIterations)  // check if the update is less than the threshold
        finished = true
      else centroids = newCentroids
      numIterations = numIterations + 1
    } while(!finished)
    println("Final centroid points:")
    printCentroids(centroids)
//    mycentroids.map(println)
    println("Iterations = "+numIterations)

    val clustered_points = input_data_points.map(point => (findClosest(point, centroids), point))
    (centroids, clustered_points)
  }

  def run_kmeans_reducebykey_tailrec(input_data_points: RDD[Vector[Double]], numK: Int, epsilon: Double) = {

    // Initialize K random centroids, these will have the form of (centroid_number, coordinates: Array[Double])
    // so they are represented as couple, where the first element is an integer and represent the index of the centroid.
    // Fix a seed to ensure reproducibility!
    var centroids =
    ((0 until numK) zip
      input_data_points.takeSample(false, numK, 42)).
      toArray

    // print the initial centroids
    println("K Center points initialized :")
    printCentroids(centroids)


    // initialize numk initial points with coordinates (0.0, 0.0, 0.0, ...)
    //      val num_features = input_data_points.take(1)(0).length
    //      println("Number of features "+ num_features)
    //      val zerosVector = Vector.fill(numK)(Vector.fill(num_features)(0.0))
    //      val initCentroids: Array[(Int, Vector[Double])] =
    //        ((0 until numK) zip zerosVector).toArray
    //
    //      initCentroids.foreach(x => println(x._1 + " " + x._2.mkString(" ")))
    //      val initCentroids: Array[(Int, Vector[Double])] =
    //        ((0 until numK).zipAll(zerosVector, -1, zerosVector)).toArray

    /**
     * Recursive part of the algorithm needed to compute the centroids
     * @param oldCentroids centroids at the previous iteration
     * @param centroids centroids at the current iteration
     * @param numIterations number of iteration needed to compute the centroids
     * @return the final centroids computed by the kmeans algorithm
     */
    @tailrec
    def run_tailrec(oldCentroids: Array[(Int, Vector[Double])],
                    centroids: Array[(Int, Vector[Double])],
                    numIterations: Int = 0): Array[(Int, Vector[Double])] = {
      if (numIterations != 0 && (meanDistance(oldCentroids, centroids) < epsilon || numIterations > maxIterations)) {
        println("Iterations = " + numIterations)
        centroids
      }
      else {
        val newCentroids = input_data_points
          .map(p => (findClosest(p, centroids), (p,1.0)))  // one is the initial weight
          .reduceByKey(weightedMeanPoint)
          .map(c => (c._1,c._2._1))
          .collect()
        run_tailrec(centroids, newCentroids, numIterations+1)
      }

    }

    val resultingCentroids = run_tailrec(centroids, centroids, 0)
    println("Final centroid points:")
    printCentroids(resultingCentroids)

    val clustered_points = input_data_points.map(point => (findClosest(point, resultingCentroids), point))
    (resultingCentroids, clustered_points)
  }


  /**
   * load data of given a specific dataset path and returns an RDD of that data
   * @param dataset_path filepath
   * @return a pair (column, RDD) where column are the columns of the dataset and RDD contains the points
   */
  def loadData(dataset_path: String, spark: SparkSession) = {

    // () load data from csv file
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(dataset_path)
    val columns = df.columns.toVector
    val input_data_rdd = df.rdd

    // print the rdd content
    //input_data_rdd.collect().foreach(println)
    println("Number of elements: " + input_data_rdd.count())

    // generate tuples
    val input_data_points = input_data_rdd.map { row =>
      //creation of an Array[Double] with the i-th row elements
      val array = row.toSeq.toVector
      array.map(x => x.toString.toDouble)
    }.cache()  //.persist(StorageLevel.MEMORY_AND_DISK)

    // print results
    //input_data_points.collect().foreach(x => println(x.mkString(" ")))
    println("Input data counts: " + input_data_points.count())

    (columns, input_data_points)
  }

  /**
   * Run the K-Means algorithm on a specific dataset and with specific params.
   * @param dataset_path filepath
   * @param epsilon threshold to stop the computation
   * @param numK number of clusters to be computed
   */
  def kmeans_naive(dataset_path: String, epsilon: Double, numK: Int, plot_data: Boolean = false, spark: SparkSession) = {
    // (A) load data
    val (columns, input_data_points) = loadData(dataset_path, spark)

    // (B) run kmeans algorithm
    val (centroids, clustered_points_rdd) = time(run_kmeans_reducebykey(input_data_points, numK, epsilon))
    val clustered_points = clustered_points_rdd.collect()

    // (C) save the results
//    writeResToCSV("kmeans_naive_clustering_results.csv", clustered_points, columns) // run il local mode
    writeResToCSV_AWS_S3("kmeans_naive_clustering_results.csv", clustered_points, columns) //run on cluster
    if(plot_data)
      savePairPlot(clustered_points, columns, img_pkg_path+"/kmeans_naive_pairplot.png") //Vector("Recency", "Frequency", "MonetaryValue")

    // (D) compute the error WSSSE (Within set sum of squared errors)
    val sorted_centroids = centroids.sortBy(centroid => centroid._1).map(centroid => centroid._2)
    val wss = computeWSS(sorted_centroids, clustered_points, spark)
    println("Within set sum of squared errors: "+wss)

    computeSummary(columns.toArray, clustered_points_rdd, spark)
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("KMeans")
      .config("spark.executor.memory", "70g")
      .config("spark.driver.memory", "50g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","16g")
      .getOrCreate()

//    // start spark session, it contains the spark context
//    val spark : SparkSession = SparkSession.builder()
//      .appName("KMeans")
//      .master("local[*]")
//      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // using spark session we do not need the spark context anymore
    //    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // (B) EXECUTION OF THE K-MEANS ALGORITHM
    val epsilon = 0.0001
    val numK = 4
    val (columns, input_data_points) = loadData(instacart_file, spark)

    println("EXECUTION OF THE FIRST VERSION OF KMEANS (GroupBy version)")
    val (resultingCentroids1, clustered_points1) = time(run_kmeans_grouby(input_data_points, numK, epsilon))
    println()

    println("EXECUTION OF THE SECOND VERSION OF KMEANS (ReduceByKey version)")
    val (resultingCentroids2, clustered_points2) = time(run_kmeans_reducebykey(input_data_points, numK, epsilon))
    println()

    println("EXECUTION OF THE THIRD VERSION OF KMEANS (Tail Recursive ReduceByKey version)")
    val (resultingCentroids3, clustered_points3) = time(run_kmeans_reducebykey_tailrec(input_data_points, numK, epsilon))
    println()

    // (C) save the results
    println("Saving pairplot....")
    savePairPlot(clustered_points1.collect(), columns, //Vector("Recency", "Frequency", "MonetaryValue")
      img_pkg_path+"/kmeans_pairplot1.png")

    // (D) compute the error WSSSE (Within set sum of squared errors)
    println("Computing WSSSE.....")
    val wss = computeWSS(resultingCentroids1.map(elem => elem._2), clustered_points1.collect(), spark)
    println("Within set sum of squared errors: "+wss)

    // (E) Elbow method computation
    println("Computing elbow method....")
    computeElbow(2, 10, input_data_points, epsilon, run_kmeans_reducebykey, spark)

    println("Test computational time for input data points....")
    test_computational_time(input_data_points, epsilon)

    // () Run complete method
    println("Run complete method.......")
    kmeans_naive(resources_pkg_path+"/test_points_10million_8cols.csv", 0.0001, 4, false, spark)
  }
}