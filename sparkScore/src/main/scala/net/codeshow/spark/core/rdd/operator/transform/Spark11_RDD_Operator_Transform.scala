package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    //coalesce算子可以扩大分区，但是如果不进行shuffle操作是不起作用的
    //所以如果想要实现扩大分区的效果，需要使用shuffle操作
    //Spark提供了一个简化的操作
    //缩减分区:coalesce,如果想要数据均衡，可以采用shuffle
    //扩大分区:repartition
    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
