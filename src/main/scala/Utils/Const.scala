/*
This files contains constant path used in the files of the project

@author Daniele Filippini
 */
package Utils

object Const {

//  // Constant variables for local mode
//  val base_path: String = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala"
//  val clustering_pkg_path: String = base_path + "/Clustering"
//  val resources_pkg_path: String = base_path + "/Resources"
//  val img_pkg_path: String = resources_pkg_path + "/Img"
//  val dataset_path = "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/dataset"

  // Constant variables for AWS S3
  val base_path: String = "s3://customer-segmentation-bucket"
  val clustering_pkg_path: String  = base_path
  val resources_pkg_path: String = base_path
  val img_pkg_path: String = base_path
  val dataset_path: String = "There is no dataset path for s3"



  // Common constants
  val gen_filename: String = resources_pkg_path + "/test_points_4col.txt"
  val gen_initpoints_filename: String = resources_pkg_path + "/test_initial_points_4col.txt"

  val inference_filename: String = resources_pkg_path + "/test_points.csv"

  val onlineretail_file: String = resources_pkg_path + "/onlineretail_data_log_scaled.csv"
  val instacart_file: String = resources_pkg_path + "/instacart_data_log_scaled.csv"
  val multicatshop_file: String = resources_pkg_path + "/multicatshop_data_log_scaled.csv"

}
