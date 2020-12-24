package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2) //, ("c", 3),
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6),
    ))
    //cogroup: connect + group
    val cgRDD = rdd1.cogroup(rdd2)
    cgRDD.collect().foreach(println)
    sc.stop()
  }
}
