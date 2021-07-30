package DataPreprocessing

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, expr, max, mean, min, stddev}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

object postprocessing {


  /**
   * From a grouby df object provide the stats of describe for each key in the groupby object.
   * @param df spark dataframe groupby object
   * @param groupby_col column that represent the cluster
   * @param stat_col column to compute statistics on
   */
  def groupby_apply_describe(df: sql.DataFrame, groupby_col: String, stat_col: String) = {
    val output = df.groupBy(groupby_col)
      .agg(count(stat_col).as(s"${stat_col}_count"),
        round(mean(stat_col),2).as(s"${stat_col}_mean"),
        round(stddev(stat_col),2).as(s"${stat_col}_std"),
        round(min(stat_col),2).as(s"${stat_col}_min"),
        round(expr(s"percentile(${stat_col}, array(0.25))")(0),2).as(s"${stat_col}_%25"),
        round(expr(s"percentile(${stat_col}, array(0.5))")(0),2).as(s"${stat_col}_%50"),
        round(expr(s"percentile(${stat_col}, array(0.75))")(0),2).as(s"${stat_col}_%75"),
        round(max(stat_col),2).as(s"${stat_col}_max"),
      )
    //    print(output.orderBy(groupby_col).show())
    output.orderBy(groupby_col).show()
  }

  def computeSummary(columns: Array[String], clustered_points_rdd: RDD[(Int, Vector[Double])],
                     spark: SparkSession) = {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    /**
     * compute the schema to create the dataframe
     * @param columns names of the df columns
     * @return
     */
    def getSchema(columns: Array[String]): StructType = {
      var schemaArray = scala.collection.mutable.ArrayBuffer[StructField]()
      for(col <- columns){
        schemaArray += StructField(col , DoubleType, true)
      }
      StructType(schemaArray)
    }

    val allcolumns = columns :+ "clusterId"  // list of all columns of the dataframe
    val mapped_rdd =  clustered_points_rdd.map(elem => Row.fromSeq(elem._2 :+ elem._1.toDouble) )
//    val df = spark.createDataFrame(mapped_rdd).toDF(allcolumns: _*)
    val schema = getSchema(allcolumns)
    val df = spark.createDataFrame(mapped_rdd, schema)

    println("show dataframe created")
    df.show()
    df.describe().show()

    // results analysis
    val group_column = "clusterId"

    println()
    println("------------------------------------------------------------------------------")
    println("---------------------------------Summary--------------------------------------")
    for( col <- columns ) {
      println("============== "+col+" ==============")
      groupby_apply_describe(df, group_column, col)
    }
    println("------------------------------------------------------------------------------")
    println("-----------------------------------End----------------------------------------")
    println()
  }

}
