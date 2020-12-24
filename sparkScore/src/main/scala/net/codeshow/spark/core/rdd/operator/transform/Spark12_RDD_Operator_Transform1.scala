package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序
    //第二个参数可以改变排序的方式
    //sortBy默认情况下不会改变分区，但是中间存在shuffle操作
    val newRDD = rdd.sortBy(t => t._1.toInt, ascending = false)
    newRDD.collect().foreach(println)
    sc.stop()
  }
}
