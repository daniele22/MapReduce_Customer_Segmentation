/*
This class contains utility functions used to manage the files, the time of execution of the models and other
common tasks.

@author Daniele Filippini
 */

package Utils

import java.io.{BufferedWriter, File, FileWriter, PrintStream, PrintWriter}

import Utils.common.printToFile
import Utils.common.time
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.AmazonServiceException

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
   * Get the time that a function needs to complete its work
   * @param block function to run
   * @tparam R return type of the function
   * @return time needed to finish the work
   */
  def getRunningTime[R](block: => R): Long = {  // lazy evaluation for block
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    // return time
    (t1 - t0)/1000000
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
  def writeResToCSV(filename: String, data: Array[(Int, Vector[Double])],
                    column_names: Vector[String] = Vector()): Unit = {
    val elemSize = data(0)._2.length
    if(column_names.nonEmpty) assert(column_names.length == elemSize)

    val dataToWrite = getColNames(column_names) ++
      data.map(x => x._2.mkString(",") + "," + x._1)
    printToFile(resources_pkg_path+"/"+filename) {
      p => dataToWrite.foreach(p.println)
    }
  }


  /**
   * This function creates a comma separated string starting from column array and the data array.
   * The string is used by the function writeResToCSV_AWS_S3 to create a new file inside the specified bucket.
   * @param data array of clustered points
   * @param column_names array of colum names, this could be empty
   * @return comma separated string with all the data
   */
  private [Utils] def computeS3FileContent(data: Array[(Int, Vector[Double])],
                           column_names: Vector[String] = Vector()) = {
    // check if the column array size is consistent with the data
    val elemSize = data(0)._2.length
    if(column_names.nonEmpty) assert(column_names.length == elemSize)

    //create comma separated string
    val colString = getColNames(column_names).mkString(",") + "\n"
    val dataString = data.map(x => x._2.mkString(",") + "," + x._1).mkString("\n")
    val res = colString + dataString

    res
  }

  /**
   * Creates a new file in the S3 bucket with the results of the clustering.
   * @param filename name of csv file
   * @param data array of clustered data
   * @param column_names array of colum names, this could be empty
   * @return Unit
   */
  def writeResToCSV_AWS_S3(filename: String, data: Array[(Int, Vector[Double])],
                             column_names: Vector[String] = Vector()) = {

    val s3Client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()

    val bucketName = Const.bucket_name
    val objectKey = filename
    val objectContent = computeS3FileContent(data, column_names)

    try{
      //create new file in the specified bucket of s3
      s3Client.putObject(bucketName, objectKey, objectContent)
    }catch {
      case e: AmazonServiceException => println("Error, AWS exception during writing result content on S3 file. (Error in function writeResToCSV_AWS_S3)"+e)
      case e: Exception => println("Error during result content on S3 file.")
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
    printToFile(resources_pkg_path+"/"+filename) {
      p => dataToWrite.foreach(p.println)
      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
    }
  }

  /**
   * this function compute a random vector of coordinates for a point
   * with the dimension equal to the num_features given as input
   * @return vector of coordinate
   */
  private [common] def getPointCoord(num_features: Int, minCoord: Double,
                                     maxCoord: Double): Vector[Double] ={
    (for {
      j <- (0 until num_features) //.par -> no benefits
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
    val fileOfPointsOut = new File(resources_pkg_path+"/"+filename)
    val bw_points = new BufferedWriter(new FileWriter(fileOfPointsOut))
    for(arr <- initial_points){
      for(coordinate <- arr){
        bw_points.write(coordinate.toString+"\t")
      }
      bw_points.write("\n")
    }
    bw_points.close()
  }

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
    val initial_points: Vector[Vector[Double]] = (for { i <- (0 until numK) }
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

    // this shoud be the coordinates of final centroids of K-Means algorithm
//    val initial_points = Vector(Vector(50.0, 50.0,50.0,50.0),
//      Vector(50.0, -50.0, 50.0, -50.0),
//      Vector(-50.0, 50.0, -50.0, 50.0),
//      Vector(-50.0, -50.0, -50, -50))

    // (2) Write the n points created to a csv file
    // This version with the for comprehension is more efficient w.r.t. the version with two nested for,
    // but in case we generate a very high number of points is too expensive in terms of memory consumption
//    val dataToWrite = for {
//      i <- (0 until numPoints).toVector.par
//      centroid = initial_points(randomPoint.nextInt(initial_points.length))
//      elem = for {
//        j <- 0 until num_features
//        feature_value = (random.nextDouble()-0.5) * distance
//      } yield centroid(j) + feature_value
//      //      j <- 0 until num_features
//      //      feature_value = (random.nextDouble()-0.5) * distance
//    } yield elem.mkString(",")
//    // print the computed data to a csv file
//    printToFile(resources_pkg_path+"/"+gen_filename) {
//      p => dataToWrite.foreach(p.println)
//      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
//    }

    // This version creates a file using two nested loops
    val file  = new File(resources_pkg_path+"/"+gen_filename)
    val bw = new BufferedWriter(new FileWriter(file))
    // gen header
    for(x <- 0 until num_features){
      if(x == num_features-1) bw.write( "Col"+x+"\n")
      else bw.write("Col"+x+",")
    }
    // gen data
    for (i <- 0 until numPoints){
      val centroid = initial_points(randomPoint.nextInt(initial_points.length))
      for(j <- 0 until num_features){
        val feature_value = (random.nextDouble()-0.5) * distance
        if(j == num_features-1) bw.write((centroid(j) + feature_value) + "")
        else bw.write((centroid(j) + feature_value) + ",")
      }
      bw.write("\n")
    }
    bw.close()
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
   * @param inputFilename file .txt with the input data
   * @param numK number of clusters
   * @param spark SparkSession
   * @param sqlContext
   * @return sql.Dataframe with the data of the txt file
   */
  def readTxtFileAndMakeDF(inputFilename: String, numK: Int,
                           spark: SparkSession, sqlContext: SQLContext): DataFrame = {
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

  /**
   * write a list to a text file, one element per row
   * @param filename name of the file in which the list will be saved
   * @param data list
   */
  def writeListToTxt(filename: String, data: Seq[String]): Unit = {
    printToFile(resources_pkg_path+"/"+filename) {
      p => data.foreach(p.println)
      //p => data.map(x => x._1 + "," + x._2).collect().foreach(p.println) }
    }
  }

  def main(args: Array[String]): Unit = {  // main used to test the common.scala functions

//    // start spark session, it contains the spark context
    val spark : SparkSession = SparkSession.builder()
      .appName("File Generation")
      .master("local[*]")
      .getOrCreate()
//
//    val sqlContext = spark.sqlContext
//    import sqlContext.implicits._
//
    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    println("GENERATION OF A FILE")
    time(genFileCsv(4, 4, "temp_initpoint_txt",
      "test_points_10million_4cols.csv",
      numPoints = 10000000))

  }

}
