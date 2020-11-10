package sparkrdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object rddlearn1 {
  val conf=new SparkConf().setAppName("rddlearn1").setMaster("local[3]")


}
