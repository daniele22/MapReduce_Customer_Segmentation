package Utils

import java.io.{BufferedWriter, File, FileWriter, PrintStream, PrintWriter}

import Clustering.KmeansII.{initializationMode, maxIterations, numClusters, numEpsilon}
import Utils.common.printToFile
import myparallel.primitives.time
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.annotation.tailrec
import scala.util.Random

import Utils.Const._

object common {

  /**
   * Record the time a function needs to complete its work
   */
  def time[R](block: => R): R = {  // lazy evaluation for block
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }

  /**
   * this function print some content to a specific file
   * @param fileName file path in which the text is going to be printed
   */
  private [Utils] def printToFile(fileName: String)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(new File(fileName))
    try {
      op(p) // write on file
    } catch {
      case e: Exception => println("An exception occur the printing to file in printToFile function")
    } finally {
      p.close()
    }
  }

  /**
   * This function adds the "Cluster" column in the case the vector of columns names is not empty
   * in this way in the resulting csv file the correct header will be present.
   * In case the column_names vector is empty in the resulting csv file the header will not be present.
   * @param column_names names of columns of the dataset
   * @return column names that must be writed to the file
   */
  private [Utils] def getColNames(column_names: Vector[String]) : Vector[String] = {
    if(column_names.nonEmpty)
      Vector( (column_names :+ "Cluster").mkString(",") )
    else
      column_names
  }

  /**
   * This function can be used to write to a csv file the results of the computation of clustering algorithms.
   * The rdd contains a couple (Array[Double, Int) where the first element is an array that contains all the coordinates
   * points and the second element is the cluster id of that point.
   * @param filename name of the file csv that will be created with the results
   * @param data data to write in the file
   * @param column_names name of the columns to use as header, this param is optional and can be empty, in this case no
   *                     header will be present in the resulting file
   */
  def writeResToCSV(filename: String, data: RDD[(Vector[Double], Int)],
                    column_names: Vector[String] = Vector()): Unit = {
    val elemSize = data.takeSample(false, 1, 42)(0)._1.length
    if(column_names.nonEmpty) assert(column_names.length == elemSize)

    val dataToWrite = getColNames(column_names) ++
      data.map(x => x._1.mkString(",") + "," + x._2).collect()
    printToFile(filename) {
      p => dataToWrite.foreach(p.println)
      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
    }
  }

  /**
   * Write an RDD[Row] to a csv file
   * @param filename name of the file csv that will be created with the results
   * @param data data to write in the file
   * @param column_names name of the columns to use as header, this param is optional and can be empty, in this case no
   *                     header will be present in the resulting file
   */
  def writeToCSV(filename: String, data: RDD[Row],
                    column_names: Vector[String] = Vector()): Unit = {
    println("run write on file csv")
    val elemSize = data.takeSample(false, 1, 42)(0).mkString(" ").split(" ").length
    if(column_names.nonEmpty) assert(column_names.length == elemSize)

    val dataToWrite = Vector(column_names.mkString(",")) ++
      data.map(x => x.mkString(",")).collect()
    println("run printing")
    printToFile(filename) {
      p => dataToWrite.foreach(p.println)
      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
    }
  }

  //TODO riguardare se questa funzione serve effettivamente a qualcosa

//  /**
//   * This function is used to write the points generated by the gen function to a csv file.
//   * @param filename
//   * @param data
//   * @param column_names
//   */
//  def writeGenFileToCSV(filename: String, data: RDD[(Array[Double], Int)], column_names: Vector[String] = Vector()): Unit = {
//    val elemSize = data.takeSample(false, 1, 42)(0)._1.length
//    if(column_names.nonEmpty) assert(column_names.length == elemSize)
//
//    val dataToWrite = getColNames(column_names) ++
//      data.map(x => x._1.mkString(",") + "," + x._2).collect()
//    printToFile(filename) {
//      p => dataToWrite.foreach(p.println)
//      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
//    }
//  }

  /**
   * this function compute a random vector of coordinates for a point
   * with the dimension equal to the num_features given as input
   * @return vector of coordinate
   */
  private [common] def getPointCoord(num_features: Int, minCoord: Double,
                                     maxCoord: Double): Vector[Double] ={
    (for {
      j <- (0 until num_features).par
    } yield minCoord+(math.random()*((maxCoord-minCoord) + 1))
      ).toVector
  }

  /**
   * function used to save the initial points (the centroids) calculated by the gen function
   * to a txt file.
   * @param filename file path in which the points will be saved
   * @param initial_points vector of the inital point, this will have dimension numK
   */
  private [common] def saveInitialPointsToTxtFile(filename: String,
                                                  initial_points: Vector[Vector[Double]]): Unit = {
    val fileOfPointsOut = new File(filename)
    val bw_points = new BufferedWriter(new FileWriter(fileOfPointsOut))
    for(arr <- initial_points){
      for(coordinate <- arr){
        bw_points.write(coordinate.toString+"\t")
      }
      bw_points.write("\n")
    }
    bw_points.close()
  }

  // generation of a file with coordinates of random points
  // num_features determines the dimension of the vectorial space

  /**
   * This function generates a file .txt that contains coordinates of random points.
   * NOTE THE MINIMUM NUMBER OF FEATURES ADMITTED IS 2
   * @param num_features determines the number of columns, so the different axis of the vectorial space in which
   *                     the points belongs
   * @param numK number of clusters that will be generated
   * @param gen_initpoints_filename the initial K point from which the clusters will be generated are saved to a
   *                                specific file, in such a way it's possible to compare the results produced by an
   *                                algorithm to this file
   * @param gen_filename path to file that contains the resulting points
   * @param numPoints number of points generated
   * @param distance distance to the cluster center that is used to generate the points
   * @param minCoord minimum coordinate
   * @param maxCoord maximum coordinate
   */
  def genFileTxt(num_features: Int, numK: Int,
                 gen_initpoints_filename: String, gen_filename: String,
                 numPoints: Int = 100000, distance: Double = 80.0,
                 minCoord: Double = -100, maxCoord: Double = 100
                ): Unit = {
    println("Generation of a new file with " + num_features + " columns")
    // minimum number of features 2
    if(num_features < 2) throw new Exception("Error num_features must be greater equal to 2")
    // generation params
    val random = new Random
    val randomPoint = new Random

    // generation of the initial points
    val initial_points: Vector[Vector[Double]] = (for { i <- (0 until numK).par }
      yield getPointCoord(num_features, minCoord, maxCoord)).toVector

    println("Init points array: ")
    println(initial_points.foreach{x => if(x != null) println(x.mkString(" ")) else println("Null")})
    println("The centroids returned by the clustering algorithms must be almost equal to these points!")

    // (1) Write the inital points to a .txt file in order to check the algorithms results
    saveInitialPointsToTxtFile(gen_initpoints_filename, initial_points)

    //val initPoints = Vector((50.0, 50.0), (50.0, -50.0), (-50.0, 50.0), (-50.0, -50.0)) // queste dovranno risultare essere anche circa le coordinate dei centroidi ottenuti con l'algoritmo di k-means

    // (2) Write random points to a .txt file
    val file  = new File(gen_filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (i <- 0 until numPoints){
      val centroid = initial_points(randomPoint.nextInt(initial_points.length))
      for(j <- 0 until num_features){
        val feature_value = (random.nextDouble()-0.5) * distance
        bw.write((centroid(j) + feature_value) + "\t")
        //bw.write((centroid._1+x)+"\t"+(centroid._2+y)+"\n")
      }
      bw.write("\n")
    }
    bw.close()
  }


  /**
   * This function generates a file .csv that contains coordinates of random points.
   * NOTE THE MINIMUM NUMBER OF FEATURES ADMITTED IS 2
   * @param num_features determines the number of columns, so the different axis of the vectorial space in which
   *                     the points belongs
   * @param numK number of clusters that will be generated
   * @param gen_initpoints_filename the initial K point from which the clusters will be generated are saved to a
   *                                specific file, in such a way it's possible to compare the results produced by an
   *                                algorithm to this file
   * @param gen_filename path to file that contains the resulting points
   * @param numPoints number of points generated
   * @param distance distance to the cluster center that is used to generate the clouds points, note
   *                 this measur influence the fact that some clouds  may overlap
   * @param minCoord minimum coordinate
   * @param maxCoord maximum coordinate
   */
  def genFileCsv(num_features: Int, numK: Int,
                 gen_initpoints_filename: String, gen_filename: String,
                 numPoints: Int = 100000, distance: Double = 80.0,
                 minCoord: Double = -100, maxCoord: Double = 100
                ): Unit = {
    println("Generation of a new file with " + num_features + " columns")
    // minimum number of features 2
    if(num_features < 2) throw new Exception("Error num_features must be greater equal to 2")
    // generation params
    val random = new Random
    val randomPoint = new Random

//    // generation of the initial points
//    val initial_points: Array[Array[Double]] = new Array[Array[Double]](numK)
//    for(i <- 0 until numK) {
//      //      println("initial points array at iteration " + i + " has size: " + initial_points.length + " with elements:")
//      //      println("Init points interno: " +i+" ")
//      //      println(initial_points.foreach{x => if(x != null) println(x.mkString(" ")) else println("Null")})
//      val elem: Array[Double] = (
//        for {
//          j <- 0 until num_features
//        } yield minCoord+(math.random()*((maxCoord-minCoord) + 1))
//        ).toArray
//      //      println("Creation of array " + i + ": "+ elem.mkString(" "))
//      initial_points(i) = elem
//    }

    // generation of the initial points, these are the points used to generate a random
    // clouds of points around these specific initial points
    val initial_points: Vector[Vector[Double]] = (for { i <- 0 until numK }
      yield getPointCoord(num_features, minCoord, maxCoord)).toVector

    println("Init points array: ")
    println(initial_points.foreach{x => if(x != null) println(x.mkString(" ")) else println("Null")})
    println("The centroids returned by the clustering algorithms must be almost equal to these points!")

    // (1) Write the "inital points" to a .txt file in order to check the algorithms results
    saveInitialPointsToTxtFile(gen_initpoints_filename, initial_points)

    //val initPoints = Vector((50.0, 50.0), (50.0, -50.0), (-50.0, 50.0), (-50.0, -50.0)) // queste dovranno risultare essere anche circa le coordinate dei centroidi ottenuti con l'algoritmo di k-means

    // (2) Write the n points created to a csv file
    val dataToWrite = for {
      i <- (0 until numPoints).toVector
      centroid = initial_points(randomPoint.nextInt(initial_points.length))
      elem = for {
        j <- 0 until num_features
        feature_value = (random.nextDouble()-0.5) * distance
      } yield centroid(j) + feature_value
      //      j <- 0 until num_features
      //      feature_value = (random.nextDouble()-0.5) * distance
    } yield elem.mkString(",")
    // print the computed data to a csv file
    printToFile(gen_filename) {
      p => dataToWrite.foreach(p.println)
      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
    }
  }


  /***
   * This function can be used to convert a file in .txt format to a .csv file
   * @param inputFilename path to file txt
   * @param outputFilename path to csv file
   */
  def readTxtFileAndMakeCsv(inputFilename: String, outputFilename: String, spark: SparkSession): Unit = {
    // read text file and split by arbitrary number of subsequent spaces
    val fileRdd = spark.sparkContext.textFile(inputFilename).map(_.split("\\s+"))
    // reformat data to obtain a comma separeted file
    val dataToWrite = fileRdd.collect().map(x => x.mkString(","))
    println("Number of elements: " + fileRdd.count() )
    // write data to file
    printToFile(outputFilename) {
      p => dataToWrite.foreach(p.println)
    }
  }

  /**
   * This function is used to read a .txt file and obtain a dataframe containing the data
   * TODO comments
   * @param inputFilename
   * @param outputFilename
   * @param numK
   * @param spark
   * @param sqlContext
   * @return
   */
  def readTxtFileAndMakeDF(inputFilename: String, outputFilename: String,
                           numK: Int, spark: SparkSession, sqlContext: SQLContext): DataFrame = {
    @tailrec
    def getSchemaStr(current_str: String, index: Int, numK: Int): String ={
      if(index == numK) current_str
      else getSchemaStr(current_str + "col"+index+" ", index+1, numK)
    }

    //OLD method to save to csv file through conversion of RDD to DataFrame
    val schemaString = getSchemaStr("", 0, numK)
    //      println("SCHEMA STRING: "+schemaString)
    val fields = schemaString.split("\\s+")
      .map(fieldName => StructField(fieldName, StringType, nullable=true))
    val schema = StructType(fields)
    val fileRdd = spark.sparkContext.textFile(inputFilename).map(_.split("\\s+")).map(x => Row.fromSeq(x))
    val df_from_file = sqlContext.createDataFrame(fileRdd, schema)
    df_from_file.show()

    // writing the csv file, this scala function creates a folder with inside the csv file
//    df_from_file
//      .coalesce(1)  // to obtain a single file
//      .write
//      .format("csv")
//      .mode("overwrite") // if the file already exists overwrite it
//      .save(outputFilename)

    df_from_file
  }


  def main(args: Array[String]): Unit = {  // main used to test the common.scala functions

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

    val prova = Array((Array(0.1, 0.2, 0.3), 1),
      (Array(0.1, 0.2, 0.3), 1),
      (Array(0.1, 0.2, 0.3), 1))
    val provaRdd = spark.sparkContext.parallelize(prova)

//    println("Saving file!")
    val base_path = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala/Clustering"
//    writeResToCSV(base_path+"/prova.csv", provaRdd, Vector("col1", "col2", "col3"))

    val inputFilename = base_path+"/test_points_4col.txt" //"C:/Users/hp/IdeaProjects/ScalableCourseExamples/src/main/scala/05_Distributed_Parallel_Programming/kmeans_points.txt"
    val outputFilename = base_path+"/test_points_prova.csv" // creates a folder
//    readTxtFileAndMakeCsv(inputFilename, outputFilename, 4, spark, sqlContext)
  }

}
