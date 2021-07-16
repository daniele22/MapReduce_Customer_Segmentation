package DataPreprocessing

import Utils.Const.{base_path, clustering_pkg_path, dataset_path, onlineretail_file}
import Utils.common.time
import org.apache.log4j.Level
import org.apache.log4j.{Logger => mylogger}
import org.apache.spark
import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.feature.{Imputer, VectorAssembler, StandardScaler => dfStandardScaler}
import Utils.common._
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql
import org.apache.spark.ml.functions.vector_to_array


object preprocessing {

  /**
   * Load the data from a csv file and print some statistics about the users
   * @param spark SparkSession variable
   * @param filepath csv file that contains the data
   * @param hasHeader boolean variables that indicates if the csv file has the header
   * @param customerCol the column of the dataframe that is used to show some information about the null values
   * @return
   */
  private [preprocessing] def loadDFandPrintInfo(spark: SparkSession, filepath: String, hasHeader: Boolean, customerCol: String): sql.DataFrame = {
    //read file
    val df = spark.read.format("csv")
      .option("header", hasHeader.toString)
      .option("mode", "DROPMALFORMED")
      .load(filepath)
      .limit(10000)  // only for testing purposes
      .cache()

    // Show some informations about the dataset
    df.show()
    df.printSchema()
    println("Number of customer : " )
    df.agg(countDistinct(customerCol).as("NumCustomers")).show()
    df.describe().show()
    println("Num of customer id with null values:" + df.filter(df(customerCol).isNull).count())

    df
  }

  /**
   * Standardize the dataframe. Standardize means normalize the data to have 0 mean and unit standard deviation.
   * @param columns dataframe columns
   * @param dataframe sql.Dataframe variable that contains the data to scale
   * @param spark SparkSession
   * @return
   */
  private [preprocessing] def scaleDataframe(columns: Array[String], dataframe: sql.DataFrame,
                                             spark: SparkSession): sql.DataFrame = {
    import spark.implicits._  // implici are needed to scale features
    val assembler = new VectorAssembler()
      .setInputCols(columns)  //Array("Recency","Frequency","MonetaryValue")
      .setOutputCol("features")

    val transformVector = assembler.transform(dataframe)

    // scaler will create two columns: the first is features that contains an array per row with the data
    // of the columns passed to the vector assembler; scaledFeatures contains an array per row with the
    // standardize data
    val scaler = new dfStandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(transformVector)
    val scaledData = scalerModel.transform(transformVector)

//    println("SHOW Another proof results")
//    scaledData.describe().show()
//    scaledData.show()
//    scaledData.printSchema()

    // Take data from scaledFeature column (these data are in array format) and dived them in many columns,
    // on for each orignial col
    val df_scaled = columns.zipWithIndex.foldLeft(scaledData)((data, col) =>
      data.withColumn(col._1, vector_to_array($"scaledFeatures").getItem(col._2))
    ).toDF()
      .drop("features", "scaledFeatures") // these are the columns created from the scaler that can be removed

    //      val df_scaled = scaledData.withColumn("Recency", vector_to_array($"scaledFeatures").getItem(0))
    //        .withColumn("Frequency", vector_to_array($"scaledFeatures").getItem(2))
    //        .withColumn("MonetaryValue", vector_to_array($"scaledFeatures").getItem(1))
    //        .drop("features", "scaledFeatures")

    df_scaled
  }

  /**
   * Save the dataframe on a csv file
   * @param filename name of the csv file, this will be save in clustering package or S3 if we are working on AWS
   * @param dataframe dataframe to save on csv file
   */
  private [preprocessing] def saveDFonFile(filename: String, dataframe: sql.DataFrame): Unit = {
    val columns = dataframe.columns.toVector
    val rdd1 = dataframe.rdd
    val file1 = clustering_pkg_path + "/" + filename
    writeToCSV(file1, rdd1, columns)
  }

  /**
   * Apply log transformation on dataframe columns, note the columns must contain numeric data.
   * @param dataframe sql.Dataframe with the data that have to be scaled
   * @param spark SparkSession
   * @return
   */
  private [preprocessing] def logScaling(dataframe: sql.DataFrame, spark: SparkSession):sql.DataFrame = {
    dataframe.createOrReplaceGlobalTempView("RFM")
    // Create the string with the query that have to be executed
    var sqlString = "SELECT "
    for (col <- dataframe.columns.zipWithIndex) {
      if(col._2 == dataframe.columns.length-1) sqlString += "log("+col._1+" + 1) as "+col._1+" "
      else sqlString += "log("+col._1+" + 1) as "+col._1+", "
    }
    sqlString += "FROM global_temp.RFM"
    val df_log = spark.sql(sqlString)
    df_log
  }

  /**
   * Impute missing values for some columns
   * @param dataframe sql.Dataframe
   * @param columns list of columns to impute
   * @param stategy the type of measure used to impute missing values, e.g. "mean", "median", ...
   * @return dataframe with imputed values
   */
  def imputeMissingValues(dataframe: sql.DataFrame, columns: Array[String], stategy: String): sql.DataFrame = {
    val imputer = new Imputer()
      .setInputCols(columns)
      .setOutputCols(columns)
      .setStrategy(stategy)

    println("Num of null values for col 0 before imputing:" + dataframe.filter(dataframe(columns(0)).isNull).count())
    val df_imputed = imputer.fit(dataframe).transform(dataframe)
    println("DF imputed")
    df_imputed.show()
    df_imputed.describe().show()
    println("Num of null values for col 0 after imputing:" + df_imputed.filter(df_imputed(columns(0)).isNull).count())

    df_imputed
  }

  //TODO modificare i nomi dei file che contengono i risultati
  /**
   * Data preprocessing pipeline for the Online Retail Dataset.
   * All the needed preprocessing steps are applied in this function: data cleaning, feature generation,
   * data transformation, ...
   * At the end the standardized dataset is saved in the file onlineretail_data_scaled.csv
   * and the log scaled dataset is saved in the file onlineretail_data_log_scaled.csv
   * @param spark
   * @param sqlContext
   */
  def run_preprocessing_onlineretail_dataset(spark: SparkSession, sqlContext: => SQLContext) ={

    import spark.implicits._

    // 0. read data from file
    val df = loadDFandPrintInfo(spark, dataset_path + "/Online_Retail_II/data.csv", hasHeader = true,
      "Customer ID")

    // 1. remove duplicate values
    val df_drop = df.dropDuplicates()
    println("DF after removing duplicates")
    df_drop.describe().show()

    // 2. remove rows where customer id is null
    val df_notnull = df_drop.filter(df_drop("Customer ID").isNotNull)
    println("DF after null entries delete")
    df_notnull.describe().show()

    // 3. remove quantities with negative values
    val df_notneg = df_notnull.filter(df_notnull("Quantity") > 0)
    println("DF after filtering rows with negative values")

    // 4. remove useless columns: stockcode, description, country
    val df_removecol = df_notneg.drop("StockCode", "Description", "Country")
    println("DF after columns drop")
    df_removecol.describe().show()

    // 5. feature generation: total price = quantity * price
    val df_newfeatures = df_removecol.withColumn("Amount",
      df_removecol.col("Price") * df_removecol.col("Quantity"))
    println("DF with Amount column")
    df_newfeatures.describe().show()

    // 6. deal with datatime.
    val df_time = df_newfeatures.withColumn("InvoiceDate",
      col("InvoiceDate").cast(TimestampType)).cache()
    println("DF change type of InvoiceDate column")
    df_time.printSchema()

    // 7. compute recency:
    //    reference date = max invoice date + 1;
    //    recency = reference date - df.invoiceDate;
    //    then goupby customer id and select the recency with the minimum value

    // To compute the max date
//    val max_date_df = df_time.agg(max(df_time.col("InvoiceDate")).as("MaxInvoiceDate"))
//    max_date_df.show()
//    val max_date = max_date_df.head().getTimestamp(0)

    // compute the reference date
    val reference_date_df = df_time.agg(max(df_time.col("InvoiceDate")).as("MaxInvoiceDate"))
      .withColumn("1_day_after", $"MaxInvoiceDate".cast("timestamp") + expr("INTERVAL 24 HOURS"))
    println("DF compute recency")
    reference_date_df.show()
    reference_date_df.printSchema()

//    val reference_date = reference_date_df.head().getTimestamp(1)
//    println("Reference date timestamp: "+reference_date)

    val reference_date_df_long = reference_date_df.withColumn("1_day_after",
      reference_date_df.col("1_day_after").cast(LongType))
    reference_date_df_long.printSchema()

    val reference_date_long = reference_date_df_long.head().getLong(1)
    println("Reference date timestamp long: "+reference_date_long)
    reference_date_df_long.show()

    val df_days = df_time.withColumn("days_since_the_last_purchase",
      abs(df_time.col("InvoiceDate").cast(LongType) - reference_date_long) / (24*3600) ).cache()


    // TODO not used
//    val df_recency = df_days.select("Customer ID", "days_since_the_last_purchase")
//      .groupBy("Customer ID")
//      .agg(min("days_since_the_last_purchase").as("Recency"))
//    println("RECENCY DF")
//    df_recency.show()
//    df_recency.describe().show()
//    //    df_recency.printSchema()
//    //    df_recency.describe().show()

    // 7b. compute together recency and monetary value in a single pass
    val df_recency_and_monetary = df_days.select("Customer ID", "days_since_the_last_purchase", "Amount")
      .groupBy("Customer ID")
      .agg(min("days_since_the_last_purchase").as("Recency"),
        sum("Amount").as("MonetaryValue"))
    println("RECENCY AND MONETARY DF")
    df_recency_and_monetary.show()
    df_recency_and_monetary.describe().show()

    // 8. compute frequency: group by user and invoice number, then count the number of elements
    // then again group by for rhe customer id
    println("DF compute frequency")
    val df_frequency = (df_days.select("Customer ID", "Invoice")
      .groupBy("Customer ID", "Invoice")
      .agg(count("*").as("Num")))
      .groupBy("Customer ID").agg(count(lit(1)).as("Frequency"))
    println("FREQUEncy df show")
    df_frequency.show()
    df_frequency.describe().show()

    // 9. compute monetary value: group by user id and sum the total amount spent in each transaction
    //    println("DF compute monetary value")
    //    val df_monetary = df.select("Customer ID", "Amount")
    //      .groupBy("Customer ID")
    //      .agg(sum("Amount").as("MonetaryValue"))
    //    println("MONETARY VALUE DF")
    //    df_monetary.show()
    //    df_monetary.describe().show()

    // 10. merge dataframes
    //val merged_cols = df_recency_and_monetary.columns.toSet ++ df_frequency.columns.toSet
    var df_rfm = df_recency_and_monetary.join(df_frequency,
      df_recency_and_monetary.col("Customer ID") === df_frequency.col("Customer ID"),
      "inner") // inner join: where keys don’t match the rows get dropped from both datasets
      .cache()
    df_rfm = df_rfm.drop("Customer ID").cache()
    println("Merging results")
    df_rfm.show()
    df_rfm.describe().show()

    // 10. feature scaling: select recency, frequency and monetary value then standardize the data
    println("FEATURE SCALING")
    val df_scaled = scaleDataframe(df_rfm.columns, df_rfm, spark)

//    def extr(x: Any, position: Int)= {
//      val el = x.getClass.getDeclaredMethod("toArray").invoke(x)
//      val array = el.asInstanceOf[Array[Double]]
//      array(position)
//    }
//    val RecencyExtractor = udf {
//      (x: Any) => {
//        extr(x, 0)
//      }
//    }
//    val FrequencyExtractor = udf {
//      (x: Any) => {
//        extr(x, 2)
//      }
//    }
//    val MonetaryExtractor = udf {
//      (x: Any) => {
//        extr(x, 1)
//      }
//    }


//    val df_new = scaledData.withColumn("Recency", RecencyExtractor(scaledData.col("scaledFeatures")))
//      .withColumn("Frequency", FrequencyExtractor(scaledData.col("scaledFeatures")))
//      .withColumn("MonetaryValue", MonetaryExtractor(scaledData.col("scaledFeatures")))
//    scaledData.createOrReplaceGlobalTempView("SCALEDDATA")
//    val df_new = spark.sql(s"SELECT array_position(scaledFeatures, 0) as Recency, " +
//      s"array_position(scaledFeatures, 2)  as Frequency, " +
//      s"array_position(scaledFeatures, 1)  as MonetaryValue FROM global_temp.SCALEDDATA")

    println("DF scaled")
    df_scaled.show()
    df_scaled.describe().show()


//    val columns = df_rfm.columns.toVector
//    val rdd_rfma = df_rfm.rdd
//    println("PROVA RFM AAAA")
//    rdd_rfma.foreach(x => println(x.toSeq.mkString(" ")))
//    val rdd_rfm = df_rfm.select(array(df_rfm.columns.map(col(_)) : _*)).rdd
//      .map(x => Vectors.dense(x.getSeq[Double](0).toArray))
//    println("RFF RFM")
//    rdd_rfm.collect().foreach(x => println(x.toArray.mkString(" ")))
//    val scaler1 = new StandardScaler(withMean = true, withStd = true).fit(rdd_rfm)
//    val rdd1 = scaler1.transform(rdd_rfm)
////    val rdd_row = rdd1.map(x => Row.fromSeq(x.toArray))
////    val d = rdd_row.toDF()
//    rdd1.collect().foreach(x => println(x.toArray.mkString(" ")))

    // 11. save the resulting df on a file
    saveDFonFile("data_scaled.csv", df_scaled)


    // 12. apply also the log scaling, to df of step 9, remember before to add 1 to each element of the dataset
    val df_log = logScaling(df_rfm, spark)
    println("Log scaled data")
    df_log.show()
    df_log.describe().show()

//    df_tmp.createOrReplaceGlobalTempView("RFM")
//    val df_tmp2 = spark.sql(s"SELECT log(2, Recency) as Recency, " +
//      "log(2, Frequency) as Frequency, " +
//      "log(2, MonetaryValue) as MonetaryValue FROM global_temp.RFM")
//    println("Log scaled data 2")
//    df_tmp2.show()
//    df_tmp2.describe().show()

    // 13. standardize the log scaled data
    val df_log_scaled = scaleDataframe(df_log.columns, df_log, spark)
    println("df log scaled results")
    df_log_scaled.show()
    df_log_scaled.describe().show()

    // 14. save results on a file
    saveDFonFile("data_log_scaled.csv", df_log_scaled)
  }

  /**
   * Data preprocessing pipeline for the Instacart Dataset.
   * All the needed preprocessing steps are applied in this function: data cleaning, feature generation,
   * data transformation, ...
   * At the end the standardized dataset is saved in the file instacart_data_scaled.csv
   * and the log scaled dataset is saved in the file instacart_data_log_scaled.csv
   * @param spark
   * @param sqlContext
   */
  def run_preprocessing_instacart_dataset(spark: SparkSession, sqlContext: SQLContext) ={

    import spark.implicits._

    // 0. read data from file
    var df = loadDFandPrintInfo(spark, dataset_path + "/Instacart/orders.csv", hasHeader = true, "user_id")
    val order_products_train_df = loadDFandPrintInfo(spark,
      dataset_path + "/Instacart/order_products__prior.csv", hasHeader = true, "product_id")
    val order_products_prior_df = loadDFandPrintInfo(spark,
      dataset_path + "/Instacart/order_products__prior.csv", hasHeader = true, "product_id")

    // 1. cast column type
    df = df.withColumn("days_since_prior_order", col("days_since_prior_order").cast(DoubleType))
//      .withColumn("user_id",col("user_id").cast(DoubleType))
//      .withColumn("order_id",col("order_id").cast(DoubleType))

    // 2. remove duplicate values
    val df_drop = df.dropDuplicates()
    println("DF after removing duplicates")
    df_drop.describe().show()

    // 3. remove rows where user id is null
    val df_notnull = df_drop.filter(df_drop("user_id").isNotNull)
    println("DF after null entries delete")
    df_notnull.describe().show()

    // 4. Fill missing values
    val df_imputed1 = imputeMissingValues(df_notnull, Array("days_since_prior_order"), "mean")

    // 5. remove useless columns: eval_set
    val df_removecol = df_imputed1.drop("eval_set").cache()
    println("DF after columns drop")
    df_removecol.describe().show()


    // 6. compute recency: we do not have the information abount the number of days since the last order,
    // so we will use the average number of days between two orders of the same customer
    val df_recency = df_removecol.select("user_id", "days_since_prior_order")
      .groupBy("user_id")
      .agg(mean("days_since_prior_order").as("Recency"))
    println("RECENCY DF")
    df_recency.show()
    df_recency.describe().show()

    // 8. compute frequency: group by user and invoice number, then count the number of elements
    // then again group by for rhe customer id
    println("DF compute frequency")
    val df_frequency = (df_removecol.select("user_id", "order_id")
      .groupBy("user_id", "order_id")
      .agg(count("*").as("Num")))
      .groupBy("user_id").agg(count(lit(1)).as("Frequency"))
    println("FREQUEncy df show")
    df_frequency.show()
    df_frequency.describe().show()

    // 9. compute monetary value: define the monetary value without the price information,
    // we will use the avg number of products ordered by each user,
    println("DF compute monetary value")

    val prod_x_ord_train = order_products_train_df.groupBy("order_id")
      .agg(max("add_to_cart_order").as("add_to_cart_order"))
      .select("order_id", "add_to_cart_order")
      .withColumn("add_to_cart_order", col("add_to_cart_order").cast(DoubleType))

    val prod_x_ord_prior = order_products_prior_df.groupBy("order_id")
      .agg(max("add_to_cart_order").as("add_to_cart_order"))
      .select("order_id", "add_to_cart_order")
      .withColumn("add_to_cart_order", col("add_to_cart_order").cast(DoubleType))

    println("show prod x ord train")
    prod_x_ord_train.show()
    prod_x_ord_train.describe().show()

    println("show prod x ord prior")
    prod_x_ord_prior.show()
    prod_x_ord_prior.describe().show()


//    val df_monetary = df_removecol.select("Customer ID", "Amount")
//      .groupBy("Customer ID")
//      .agg(sum("Amount").as("MonetaryValue"))
//    println("MONETARY VALUE DF")
//    df_monetary.show()
//    df_monetary.describe().show()

    val orders_details_df = prod_x_ord_train.union(prod_x_ord_prior)
    println("show order details")
    orders_details_df.show()

    var df_joined = df_removecol.join(orders_details_df,
      Seq("order_id"),
      "inner") // inner join: where keys don’t match the rows get dropped from both datasets
      .cache()

    println("Show df joined")
    df_joined.show()
    df_joined.describe().show()

    val df_imputed2 = imputeMissingValues(df_joined, Array("add_to_cart_order"), "mean")
//    val imputer2 = new Imputer()
//      .setInputCols(Array("add_to_cart_order"))
//      .setOutputCols(Array("add_to_cart_order"))
//      .setStrategy("mean")
//
//    println("Num of days with null values before imputing:" + df_joined.filter(df_joined("add_to_cart_order").isNull).count())
//    val df_imputed2 = imputer2.fit(df_joined).transform(df_joined)
//    println("DF imputed")
//    df_imputed2.show()
//    df_imputed2.describe().show()
//    println("Num of days with null values after imputing:" + df_imputed2.filter(df_imputed2("add_to_cart_order").isNull).count())

    val df_monetary = df_imputed2.select("user_id", "add_to_cart_order")
      .groupBy("user_id")
      .agg(mean("add_to_cart_order").as("MonetaryValue"))
    println("Monetay show")
    df_monetary.show()
    df_monetary.describe().show()

    // 10. merge dataframes
    //val merged_cols = df_recency_and_monetary.columns.toSet ++ df_frequency.columns.toSet
    val df_rf = df_recency.join(df_frequency,
      Seq("user_id"),
      "inner") // inner join: where keys don’t match the rows get dropped from both datasets
      .cache()
    println("df_rf")
    df_rf.show()
    df_rf.describe().show()

    var df_rfm = df_rf.join(df_monetary,
      df_rf.col("user_id") === df_monetary.col("user_id"),
      "inner") // inner join: where keys don’t match the rows get dropped from both datasets
      .cache()
    df_rfm = df_rfm.drop("user_id").cache()
    println("Merging results")
    df_rfm.show()
    df_rfm.describe().show()

    // 10. feature scaling: select recency, frequency and monetary value then standardize the data
//    println("FEATURE SCALING")
    val df_scaled = scaleDataframe(df_rfm.columns, df_rfm, spark)
    println("DF scaled")
    df_scaled.show()
    df_scaled.describe().show()

    // 11. save the resulting df on a file
    saveDFonFile("data_scaled.csv", df_scaled)

    // 12. apply also the log scaling, to df of step 9, remember before to add 1 to each element of the dataset
    val df_log = logScaling(df_rfm, spark)
    println("Log scaled data")
    df_log.show()
    df_log.describe().show()

    // 13. standardize the log scaled data
    val df_log_scaled = scaleDataframe(df_log.columns, df_log, spark)
    println("df log scaled results")
    df_log_scaled.show()
    df_log_scaled.describe().show()

    // 14. save results on a file
    saveDFonFile("data_log_scaled.csv", df_log_scaled)

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

//    println(time(run_preprocessing_onlineretail_dataset(spark, sqlContext)))

    //TODO generalization of the prerpocessing finction


    //TODO build preprocessing for the other two datasets

    println(time(run_preprocessing_instacart_dataset(spark, sqlContext)))
  }

}
