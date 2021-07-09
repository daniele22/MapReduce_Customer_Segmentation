# DBSCAN on Spark

### Overview

This is an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) on top of [Apache Spark](http://spark.apache.org/). It is loosely based on the paper from He, Yaobin, et al. ["MR-DBSCAN: a scalable MapReduce-based DBSCAN algorithm for heavily skewed data"](http://www.researchgate.net/profile/Yaobin_He/publication/260523383_MR-DBSCAN_a_scalable_MapReduce-based_DBSCAN_algorithm_for_heavily_skewed_data/links/0046353a1763ee2bdf000000.pdf). 


There is also a [visual guide](http://www.irvingc.com/visualizing-dbscan) that explains how the algorithm works.

The content of this folder is taken from the [dbscan_spark](https://github.com/alitouka/spark_dbscan) project made by [alitokua](https://github.com/alitouka) and available on github. The App has been included in the project and tested to evaluate it in the field of customer segmentation.

	


### Example usage 

The following should give you a
good idea of how it should be used in your application.

```scala
val sc = new SparkContext("spark://master:7077", "My App")

val data = IOHelper.readDataset(sc, "/path/to/my/data.csv")

val clusteringSettings = new DbscanSettings().withEpsilon(25).withNumberOfPoints(30)

val model = Dbscan.train (data, clusteringSettings)

IOHelper.saveClusteringResult(model, "/path/to/output/folder")

val predictedClusterId = model.predict(new Point (100, 100))
println(predictedClusterId)
```

### License

DBSCAN on Spark is available under the Apache 2.0 license. 
See the [LICENSE](https://www.apache.org/licenses/LICENSE-2.0.html) file for details.
