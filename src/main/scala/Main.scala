/*
Main class that can be used to run the application through the comand shell and the spark-submit command.
Here are specified all the arguments accepted by the app that are used to run the different algorithms on different dataset.

@author Daniele Filippini
 */

import Main.{listToOptionMap, usage}
import Utils.Const._
import Clustering.KMeans.kmeans_naive
import Clustering.KmeansII.{Scalable_KMeans, kmeans}
import Clustering.DBSCAN_Distributed.dbscan
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark.sql.SparkSession
import scala.util.Try

import scala.sys.exit

object Main {

  val usage = """
    Usage: path/to/file.jar [--alg-name name] [--dataset name] [--numK num] [--epsilon num] [--minPts num] [--make-plot bool] [--file]
    Help:
      --alg-name: Name of the algorithm to run, allowed "kmeans-naive", "kmeans", "scalable-kmeans", "dbscan"
      --dataset: Name of the dataset to analyse, allowed "onlineretail", "instacart", "multicatshop"
      --numK: Number of clusters to define, this is needed only in case of kmeans algorithm
      --epsilon: This represents the stopping threshold in case of kmeans or the local radius for expanding clusters in dbscan
      --minpts: Minimum number of points to form a cluster, this is needed only for dbscan algorithm
      --make-plot: If make or not a pairplot with the results of clustering, this is not recommended for very large datasets as it is extremely expensive. Accepted values "true" and "false"
      --file: file path to test the model on a specific file if you do not want to use one of the default dataset, this file must be in csv format
  """

  val dataset_names = Seq("onlineretail", "instacart", "multicatshop")
  val alg_names = Seq("kmeans-naive", "kmeans", "scalable-kmeans", "dbscan")

  type OptionMap = Map[Symbol, Any]

  /**
   * Read argument list and define a map with the input given by the user
   * @param list arguments
   * @return map Key -> value
   */
  def listToOptionMap(list: List[String]): OptionMap = {

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {  // here we use pattern matching to get the information
        case Nil => map
        case "--alg-name" :: value :: tail =>
          nextOption(map ++ Map('algname -> value), tail)
        case "--dataset" :: value :: tail =>
          nextOption(map ++ Map('dataset -> value), tail)
        case "--numK" :: value :: tail =>
          nextOption(map ++ Map('numK -> value.toInt), tail)
        case "--epsilon" :: value :: tail =>
          nextOption(map ++ Map('epsilon -> value.toDouble), tail)
        case "--minpts" :: value :: tail =>
          nextOption(map ++ Map('minpts -> value.toInt), tail)
        case "--make-plot" :: value :: tail =>
          nextOption(map ++ Map('makeplot -> value), tail)
        case "--file" :: value :: tail =>
          nextOption(map ++ Map('file -> value), tail)
        //        case string :: opt2 :: tail if isSwitch(opt2) =>
        //          nextOption(map ++ Map('infile -> string), list.tail)
        //        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => println("Unknown option: "+option+"\n"+usage)
          exit(1)
      }
    }

    val options = nextOption(Map(), list)
    options
  }

  /**
   * Read the symbol 'dataset from the map and return its value
   * @param map arg map
   * @return dataset name
   */
  def getDatasetVal(map: OptionMap): String = {
    map.get('dataset) match {
      case None => throw new Exception("Error in dataset definition."+"\n"+usage)
      case Some(value) => val name = value.toString
        if(name.isEmpty) throw new Exception("Error invalid dataset name: "+name+"\n"+usage)
        if (dataset_names.contains(name)) name  // return the name of the dataset
        else throw new Exception("Error invalid dataset name: "+name+"\n"+usage)
    }
  }


  /**
   * Get dataset path starting from dataset name
   * @param map inputs by the user
   * @return dataset path
   */
  def getDatasetPath(map: OptionMap): String = {
    val file = map.get('file)
    file match {
      // no input file there must be a dataset name
      case None => val name = getDatasetVal(map)
        name match {
          case "onlineretail" => onlineretail_file
          case "instacart" => instacart_file
          case "multicatshop" => multicatshop_file
        }
      // use file path in input
      case Some(value) => value.toString
    }
  }

  /**
   * Read the symbol 'algname from the map and return its value
   * @param map args map
   * @return algorithm name
   */
  def getAlgorithmVal(map: OptionMap): String = {
    map.get('algname) match {
      case None => throw new Exception("Error algorithm name not defined."+"\n"+usage)
      case Some(value) => val name = value.toString
        if(name.isEmpty) throw new Exception("Error invalid algorithm name: "+name+"\n"+usage)
        if (alg_names.contains(name)) name  // return the name of the dataset
        else throw new Exception("Error invalid algorithm name: "+name+"\n"+usage)
    }
  }

  /**
   * Read the symbol epsilon from the map and return its value
   * @param map args map
   * @return epsilon value
   */
  def getEpsilonVal(map: OptionMap): Double = {
    map.get('epsilon) match {
      case None => throw new Exception("Error epsilon param not defined."+"\n"+usage)
      case Some(value) => val epsilon = value.toString
        if (epsilon.isEmpty) throw new Exception("Error in epsilon param definition, found null value."+"\n"+usage)
        else epsilon.toDouble
    }
  }

  /**
   * Read the symbol numK from the map and return its valu
   * @param map args map
   * @return number of clusters value
   */
  def getNumKVal(map: OptionMap): Int = {
    map.get('numK) match {
      case None => throw new Exception("Error numK param not defined."+"\n"+usage)
      case Some(value) => val epsilon = value.toString
        if (epsilon.isEmpty) throw new Exception("Error in numK param definition, found null value."+"\n"+usage)
        else epsilon.toInt
    }
  }

  /**
   * Read the symbol minpts from the map and return its value
   * @param map args map
   * @return min points number
   */
  def getMinPtsVal(map: OptionMap): Int = {
    map.get('minpts) match {
      case None => throw new Exception("Error minpts param not defined."+"\n"+usage)
      case Some(value) => val epsilon = value.toString
        if (epsilon.isEmpty) throw new Exception("Error in minpts param definition, found null value."+"\n"+usage)
        else epsilon.toInt
    }
  }

  /**
   * Read the symbol makeplot from the map and return its value or false as default
   * @param map args map
   * @return min points number
   */
  def getMakePlot(map: OptionMap): Boolean = {
    map.get('makeplot) match {
      case None => false
      case Some(value) => Try(value.toString.toBoolean).getOrElse(false)
    }
  }

  def callDBSCAN(dataset_path: String, map: OptionMap, spark: SparkSession): Unit = {
    val epsilon = getEpsilonVal(map)
    val minpts = getMinPtsVal(map)
    val makeplot = getMakePlot(map)

    dbscan(dataset_path, epsilon, minpts, makeplot, spark)

  }

  def callKMeans(algorithm: String, dataset_path: String, map: Main.OptionMap, spark: SparkSession): Unit = {
    val numK = getNumKVal(map)
    val epsilon = getEpsilonVal(map)
    val makeplot = getMakePlot(map)

    algorithm match {
      case "kmeans-naive" => kmeans_naive(dataset_path, epsilon, numK, makeplot, spark)
      case "kmeans" => kmeans(dataset_path, epsilon, numK, makeplot, spark)
      case "scalable-kmeans" => Scalable_KMeans(dataset_path, epsilon, numK, makeplot, spark)
    }
  }

  def main(args: Array[String]): Unit = {
    println("Running application...........")

    val spark: SparkSession = SparkSession.builder()
//      .master("local[*]")
      .appName("CustomerSegmentationApp")
      .getOrCreate()
    println("Setted spark session....")

    val rootLogger = mylogger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("WARN")

    if (args.length == 0) println(usage)
    val arglist = args.toList

    // read the inputs arguments passed by the user
    val options = listToOptionMap(arglist)
    println(options)

    val dataset_path = getDatasetPath(options)
    val algorithm = getAlgorithmVal(options)

    // run the clustering algorithm
    println("Calling the algorithm....")
    if(algorithm.equals("dbscan")) callDBSCAN(dataset_path, options, spark)
    else callKMeans(algorithm, dataset_path, options, spark)

  }



}
