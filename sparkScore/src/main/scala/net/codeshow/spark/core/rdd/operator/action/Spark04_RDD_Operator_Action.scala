package net.codeshow.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val intToLong = rdd.countByValue()
    println(intToLong)
    sc.stop()
  }

}
