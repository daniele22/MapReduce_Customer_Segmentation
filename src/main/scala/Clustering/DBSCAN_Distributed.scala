package Clustering

import ScalaSparkDBSCAN._
import ScalaSparkDBSCAN.dbscan.ClusterId
import ScalaSparkDBSCAN.spatial.Point
import ScalaSparkDBSCAN.util.io.IOHelper
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.sql.SparkSession
//import plotly._
//import plotly.element._
//import plotly.layout._
//import plotly.Plotly._
//import breeze.plot._
import ScalaSparkDBSCAN.exploratoryAnalysis.DistanceToNearestNeighborDriver
import org.apache.spark.rdd.RDD



object DBSCAN_Distributed extends java.io.Serializable{

  val base_path = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala"
  //val inference_filename: String = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/dataset/Online_Retail_II/customer_data.csv"
  val inference_filename: String = base_path+"/Clustering/test_points.csv"
  val img_filepath = base_path+"/Img"

  // Parameters of the model
  val minPts = 6
  val epsilon = 3

  def numberOfClusters(model: DbscanModel): Int = {
    val clusterIdsRdd: RDD[ClusterId] = model.allPoints.map(_.clusterId) // get the clusterid assigned to each point
      .distinct()  // get a new RDD containing the distinct elements in this RDD
      .filter(clusterId => clusterId != DbscanModel.NoisePoint) // remove the clusterid equal to that assigned to noise point if present
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

  def countPointsPerCluster(model: DbscanModel): RDD[(ClusterId, Int)] = {
    val clusteredPoints = model.clusteredPoints
    clusteredPoints.map(point => (point.clusterId, 1)).reduceByKey(_+_)  // points per cluster
  }


  def main(args: Array[String]): Unit = {

    // start spark session, it contains the spark context
    val spark : SparkSession = SparkSession.builder()
      .appName("KMeans")
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    println("Read csv file")
    // Read csv file with the IOHelper class
    val data = IOHelper.readDataset(spark.sparkContext, inference_filename, hasHeader = true)

    println("Set dbscan settings")
    // Specify parameters of the DBSCAN algorithm using SparkSettings class
    val clusteringSettings = new DbscanSettings().withEpsilon(epsilon).withNumberOfPoints(minPts)

    println("Start training the model")
    val t0 = System.nanoTime()
    // Run clustering algorithm
    val model = Dbscan.train(data, clusteringSettings)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")

    println("Number of clusters identified by the model: " + numberOfClusters(model))

    println("Number of clusterd points: "+ model.clusteredPoints)

    val pointsPerCluster = countPointsPerCluster(model)
    println("Number of points per cluster:")
    pointsPerCluster.foreach(println)

    val noisePoints = model.noisePoints
    val numOfNoisePoints = noisePoints.count().toInt
    println("Number of noise points: " + numOfNoisePoints)

    val triple = DistanceToNearestNeighborDriver.run(data, clusteringSettings)
    println("Triple:")
    println(triple)

    val X = triple.map(element => "(" + element._1.toString + " - " + element._1.toString + ")")
    val Y = triple.map(_._2)
    Plot.saveBarChart(Y, X, img_filepath+"/dbscan_barchart.png")
    
//    val XY = X zip Y
//    val barplot_data = Seq(
//      Bar(X, Y)
//    )

//    val f = Figure()
//    val p = f.subplot(0)
//    val x = linspace(0.0,1.0)
//    p += plot(x, x)
//    p += plot(x, x, '.')
//    p.xlabel = "x axis"
//    p.ylabel = "y axis"
//    f.saveas("lines.png")

//    val f = Figure()
//    val p = f.subplot(0)
//    plot(XY.toVector, XY.toVector, '-', name = "Barplot" )
//    f.saveas("prova_plot.png")
//    val lay = Layout().withTitle("Curves")
//    Plotly.plot("div-id", barplot_data, layout = lay)


    /*
    Save clustering result.
    - This call will create a folder which will contain multiple partXXXX files.
    - If you concatenate these files, you will get a CSV file.
    - Each record in this file will contain coordinates of one point
      followed by an identifier of a cluster which this point belongs to.
    - For noise points, cluster identifier is 0.
    - The order of records in the resulting CSV file will be different from your input file.
    - You can save the data to any path which RDD.saveAsTextFile method will accept.

     IOHelper.saveClusteringResult(model, "/path/to/output/folder")
     */
//    IOHelper.saveClusteringResult(model, base_path)

    // Predict clusters for new points:
//    val predictedClusterId = model.predict(new Point (100, 100))
//    println (predictedClusterId)
  }

}
