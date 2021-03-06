package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    //coalesce方法默认情况下不会将分区的数据打乱重新组合
    //这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想要让数据均衡，可以进行shuffle处理
    //    val newRDD = rdd.coalesce(2)
    val newRDD = rdd.coalesce(2, shuffle = true)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
