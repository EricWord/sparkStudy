package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子
    val rdd = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)

    //分组和分区没有必然的关系
    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
