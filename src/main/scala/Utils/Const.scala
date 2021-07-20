/*
This files contains constant path used in the files of the project

@author Daniele Filippini
 */
package Utils

object Const {

  val base_path: String = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala"
  val clustering_pkg_path: String = base_path + "/Clustering"
  val resources_pkg_path: String = base_path + "/Resources"
  val img_pkg_path: String = resources_pkg_path + "/Img"

  val gen_filename: String = resources_pkg_path + "/test_points_4col.txt"
  val gen_initpoints_filename: String = resources_pkg_path + "/test_initial_points_4col.txt"

  val inference_filename: String = resources_pkg_path + "/test_points.csv"

  val dataset_path = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/dataset"
  val onlineretail_file: String = dataset_path + "/Online_Retail_II/customer_data_log_scaled.csv"
  val instacart_file: String = dataset_path + "/Online_Retail_II/customer_data_log_scaled.csv"

  //TODO aggiungere costanti x i file creati

}
