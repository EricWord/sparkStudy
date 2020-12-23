package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子-map
    val rdd = sc.makeRDD(List(1, 2, 3))

    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )
    mpRDD.collect().foreach(println)

    sc.stop()
  }

}
