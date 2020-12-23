package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子-map
    val rdd = sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    ))

    val flatRDD = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }

}
