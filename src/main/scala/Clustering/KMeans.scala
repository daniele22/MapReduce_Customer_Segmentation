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


//spark session with schema
import org.apache.spark.sql.types._


object KMeans extends java.io.Serializable{

  //val filename = "C:/Users/hp/IdeaProjects/ScalableCourseExamples/src/main/scala/05_Distributed_Parallel_Programming/kmeans_points.txt"
  // Online Retail dataset file path
  //val filename = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/dataset/Online_Retail_II/customer_data.csv"
  // Test synthetic points file path
  val base_path = "C:/Users/hp/IdeaProjects/MapReduce_Customer_Segmentation/src/main/scala/Clustering"
  val gen_filename: String = base_path+"/test_points_4col.txt"
  val gen_initpoints_filename: String = base_path+"/test_initial_points_4col.txt"
  val inference_filename: String = base_path+"/test_points.csv"
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

  // this function computes the euclidean distance between two points
  def euclideanDistance(p1: Array[Double], p2: Array[Double]) = {
    math.sqrt((p1 zip p2).map{
      case (elem1, elem2) => math.pow(elem2 - elem1, 2)  // without case it does not work because the compiler is not able to type the variables elem1 and elem2
      case _ => throw new Exception("Error in euclidean distance calculation.")
    }.sum)
  }

  // function to find the nearest centroid
  // it takes as input a point and the list of all the centroids
  // the function will return the number of the nearest centroid
  def findClosest(p: Array[Double],
                  centroids: Array[(Int, Array[Double])]): Int =
    centroids.map(c => (c._1, euclideanDistance(c._2, p))). // calculate the distance between p and each one of the centroids
      minBy(_._2)._1 // take the centroid with the smallest euclidean distance and return its number

  // computes the weighted mean of a single feature of two different points
  def weightedMean(p1_attribute: Double, p1_weight: Double, p2_attribute: Double, p2_weight: Double): Double = {
    1.0/(1.0+p2_weight/p1_weight)*p1_attribute + 1.0/(1.0+p1_weight/p2_weight)*p2_attribute
  }



  // this function computes the weighted mean of two different points, at the beginning the weight associated to each
  // point will be equal to 1, and then at each iteration the weights are added.
  // We need this approach to calculate the mean point in an efficient way in a distributed context,
  // we do not want a sequential implementation that is more slow.
  def weightedMeanPoint(p1: (Array[Double], Double),
                        p2: (Array[Double], Double)): (Array[Double], Double) = {
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

  // this function computes the mean distance between the centroids before the update and the new centroids computed.
  def meanDistance(old_centroids: Array[(Int, Array[Double])],
                   new_centroids: Array[(Int, Array[Double])]) =
    ((old_centroids zip new_centroids).
      map (c => euclideanDistance(c._1._2, c._2._2)).
      sum) / old_centroids.length



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
    val df = spark.read.format("csv").option("header", "false").option("mode", "DROPMALFORMED").load(inference_filename)
    val input_data_rdd = df.rdd
    // this print the rdd content
    //input_data_rdd.collect().foreach(println)
    println("Number of elements: " + input_data_rdd.count())
    // generate tuples
    val input_data_points = input_data_rdd.map { row =>
      //creation of an Array[Double] with the i-th row elements
      val array = row.toSeq.toArray
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


    def run1(input_data_points: RDD[Array[Double]]) = {

      // Initialize K random centroids, these will have the form of (centroid_number, coordinates: Array[Double])
      // so they are represented as couple, where the first element is an integer and represent the index of the centroid.
      // Fix a seed to ensure reproducibility!
      var centroids =
      ((0 until numK) zip
        input_data_points.takeSample(false, numK, 42)).
        toArray

      // This function print the centroids
      def printCentroids(centroids: Array[(Int, Array[Double])]): Unit = {
        for ((cen, elementsList) <- centroids){
          print("Centroid # " + cen + ": (")
          for ((element, index) <- elementsList.zipWithIndex) {
            if (index == (elementsList.length-1)) print(element)
            else print(element + ",")
          }
          println(")")
        }
      }

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
    def run2(input_data_points: RDD[Array[Double]]) = {

      // Initialize K random centroids, these will have the form of (centroid_number, coordinates: Array[Double])
      // so they are represented as couple, where the first element is an integer and represent the index of the centroid.
      // Fix a seed to ensure reproducibility!
      var centroids =
      ((0 until numK) zip
        input_data_points.takeSample(false, numK, 42)).
        toArray

      // This function print the centroids
      def printCentroids(centroids: Array[(Int, Array[Double])]): Unit = {
        for ((cen, elementsList) <- centroids){
          print("Centroid # " + cen + ": (")
          for ((element, index) <- elementsList.zipWithIndex) {
            if (index == (elementsList.length-1)) print(element)
            else print(element + ",")
          }
          println(")")
        }
      }

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
      printCentroids(centroids)
      //mycentroids.map(println)
      println("Iterations = "+numIterations)
    }

    // generation of a file with synthetic data
//    genFile(4)

    // Execution of the K-MEANS algorithm
    // we pass the centroids in input to the two functions to have the same centroids and compare the results.
    println("EXECUTION OF THE FIRST VERSION OF KMEANS (GroupBy version)")
    time(run1(input_data_points)) // versione con groupbykey
    println("EXECUTION OF THE SECOND VERSION OF KMEANS (ReduceByKey version)")
    time(run2(input_data_points))  // versione con reducebykey

  }
}