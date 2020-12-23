package net.codeshow.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD = rdd.filter(_ % 2 != 0)
    filterRDD.collect().foreach(println)


    sc.stop()
  }
}
