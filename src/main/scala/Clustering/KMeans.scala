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
import Utils.Const._

import Clustering.Plot.savePairPlot
import com.esotericsoftware.minlog.Log.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.json4s.scalap.scalasig.ClassFileParser.header

import scala.util.Random
import myparallel.primitives._
import org.apache.log4j.{Logger => mylogger}
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec



//spark session with schema
import org.apache.spark.sql.types._


object KMeans extends java.io.Serializable{


  val epsilon = 0.0001
  val numK = 4 // clusters number
//  val randomX = new Random
//  val randomY = new Random
//  val maxCoordinate = 100.0



  // compute the distance between two points
  //  def distance(p1: (Double, Double), p2: (Double, Double)) =
  //    math.sqrt(
  //      math.pow(p2._1-p1._1, 2) +
  //        math.pow(p2._2-p1._2, 2)
  //    )

  /**
   * Compute the euclidean distance between two points. The points are represented through
   * vectors, each element of the vector represent a specific dimension
   * @param p1 vector of coordinates of the first point
   * @param p2 vector of coordinates of the second point
   * @return
   */
  def euclideanDistance(p1: Vector[Double], p2: Vector[Double]) = {
    math.sqrt((p1 zip p2).map{
      case (elem1, elem2) => math.pow(elem2 - elem1, 2)  // without case it does not work because the compiler is not able to type the variables elem1 and elem2
      case _ => throw new Exception("Error in euclidean distance calculation.")
    }.sum)
  }

  /**
   * Find the nearest centroid of a specific point.
   * @param p vector of coordinates of a point
   * @param centroids list of all centroids coordinates
   * @return the index of the nearest centroid
   */
  def findClosest(p: Vector[Double],
                  centroids: Array[(Int, Vector[Double])]): Int =
    centroids.map(c => (c._1, euclideanDistance(c._2, p))). // calculate the distance between p and each one of the centroids
      minBy(_._2)._1 // take the centroid with the smallest euclidean distance and return its number

  /**
   * Compute the weighted mean of a single feature of two different points
   * @param p1_attribute an attribute of the first point (dimension of the vectorial space)
   * @param p1_weight weight associated to the first point
   * @param p2_attribute the same attribute of the second point
   * @param p2_weight weight associated to the second point
   * @return the weighted mean of that specific attribute
   */
  def weightedMean(p1_attribute: Double, p1_weight: Double, p2_attribute: Double, p2_weight: Double): Double = {
    1.0/(1.0+p2_weight/p1_weight)*p1_attribute + 1.0/(1.0+p1_weight/p2_weight)*p2_attribute
  }



  //
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
  def weightedMeanPoint(p1: (Vector[Double], Double),
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
  def meanDistance(old_centroids: Array[(Int, Vector[Double])],
                   new_centroids: Array[(Int, Vector[Double])]) =
    ((old_centroids zip new_centroids).
      map (c => euclideanDistance(c._1._2, c._2._2)).
      sum) / old_centroids.length

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

  def run_kmeans_grouby(input_data_points: RDD[Vector[Double]]) = {

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
        // compute the new position of the centroids, the first map phase creates a cuple and associates
        // the initial weight of 1.0, then a reduce phase is executed to compute the mean point
        .map(x => (x._1, (x._2.map((_, 1.0))).reduce(weightedMeanPoint)))
        .map(c => (c._1, c._2._1))  // create a couple with c_index and point coordinates, without the weight added before
        .collect()
      // compare the new centroids with the old ones
      // continue the loop until the change between two iterations is less than a specific threshold
      if (meanDistance(centroids, newCentroids) < epsilon)
        finished = true
      else centroids = newCentroids
      numIterations = numIterations + 1
    } while(!finished)
    println("Final centroid points:")
    printCentroids(centroids)
    //mycentroids.map(println)
    println("Iterations = "+numIterations)
  }


  // In this second version reduceByKey is used instead of groupBy, this should make this second version more efficient.
  // With this second version also problems related to the memory consumption can be avoided, indeed
  // all the points are aggregated on a single node with groudBy and in the case of millions of date this could be
  // a problem. With reduceByKey we can avoid that phenomenon.
  def run_kmeans_reducebykey(input_data_points: RDD[Vector[Double]]) = {

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
      Then only those data (i.e. numK*numNodes) have to travel on the network twards the main node
      This means a minor use of the network and so less time needed.
       */
      val newCentroids = input_data_points
        .map(p => (findClosest(p, centroids), (p,1.0)))  // one is the initial weight
        .reduceByKey(weightedMeanPoint)
        .map(c => (c._1,c._2._1))
        .collect()
      // compare the new centroid with the old ones
      if (meanDistance(centroids,newCentroids) < epsilon)  // check if the update is less than the threshold
        finished = true
      else centroids = newCentroids
      numIterations = numIterations + 1
    } while(!finished)
    println("Final centroid points:")
    printCentroids(centroids)
    //mycentroids.map(println)
    println("Iterations = "+numIterations)

    val clustered_points = input_data_points.map(point => (point, findClosest(point, centroids))).collect()
    savePairPlot(clustered_points, Vector("Recency", "Frequency", "MonetaryValue"),
      img_pkg_path+"/dbscan_NumberOfPointsWithinDistance.png")
  }

  def run_kmeans_reducebykey_tailrec(input_data_points: RDD[Vector[Double]]) = {
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
      if (numIterations != 0 && meanDistance(oldCentroids, centroids) < epsilon) {
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
  }



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


//    val dfWithSchema = spark.read.option("header", "true")
//      .schema(schema)
//      .csv(temp_filename)
//    dfWithSchema.show()
//
//    print("SEP")
//    file.map(_.split("\\s+")).foreach(x => println("element:" + x.toString))
//    val fileToDf = file.map(_.split(" ")).map{ case Array(a,b) => (a.toString.toDouble, b.toString.toDouble) }.toDF("col1", "col2")
//    fileToDf.show()
    //fileToDf.foreach(println(_))

    // () load data from csv file
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(onlineretail_file)
//      .load(inference_filename)
    val input_data_rdd = df.rdd
    // this print the rdd content
    //input_data_rdd.collect().foreach(println)
    println("Number of elements: " + input_data_rdd.count())
    // generate tuples
    val input_data_points = input_data_rdd.map { row =>
      //creation of an Array[Double] with the i-th row elements
      val array = row.toSeq.toVector
      array.map(x => x.toString.toDouble)
    }.cache//.persist(StorageLevel.MEMORY_AND_DISK)
    // print results
    //input_data_points.collect().foreach(x => println(x.mkString(" ")))
    println("Input data counts: " + input_data_points.count())

    // Default: load txt file made by gen function.
    //    val input = spark.sparkContext.textFile("C:/Users/hp/IdeaProjects/ScalableCourseExamples/src/main/scala/05_Distributed_Parallel_Programming/kmeans_points.txt")
    //    val sparkPoints = input.map(s =>
    //      ( (s.takeWhile(_ != '\t')).toDouble,
    //        (s.dropWhile(_ != '\t')).toDouble )
    //    ).cache//.persist(StorageLevel.MEMORY_AND_DISK)
    //    // prent results
    //    input.collect().foreach(println)
    //    println(sparkPoints.count())

    //    val rddFromFile = sc.textFile(filename)
    //    val myrdd = sc.textFile(filename)
    //      .map(line => line.split(","))
    //      .filter(line => line.length <= 1)
    //      .collect()
    //    val rdd = rddFromFile.map(f => {f.split(",")})
    //    rdd.foreach(f=>{
    //      println("Col1:"+f(0)+",Col2:"+f(1))
    //    })



    // GENERATION OF A FILE WITH SYNTHETIC DATA
//    genFile(4)

    // EXECUTION OF THE K-MEANS ALGORITHM
//    println("EXECUTION OF THE FIRST VERSION OF KMEANS (GroupBy version)")
//    time(run_kmeans_grouby(input_data_points)) // versione con groupbykey
//    println()

    println("EXECUTION OF THE SECOND VERSION OF KMEANS (ReduceByKey version)")
    time(run_kmeans_reducebykey(input_data_points))  // versione con reducebykey
    println()

//    println("EXECUTION OF THE THIRD VERSION OF KMEANS (Tail Recursive ReduceByKey version)")
//    time(run_kmeans_reducebykey_tailrec(input_data_points))  // versione con reducebykey
//    println()
  }
}