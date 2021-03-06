package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子-map
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD = rdd.glom()
    glomRDD.collect().foreach(data => println(data.mkString(",")))
    sc.stop()
  }
}
