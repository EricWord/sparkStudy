package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.map((_, 1))

    //partitionBy 根据指定的分区规则对数据进行重分区
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    sc.stop()
  }
}
