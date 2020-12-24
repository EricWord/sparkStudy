package net.codeshow.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    val stringToLong = rdd.countByKey()
    println(stringToLong)


    sc.stop()
  }

}
