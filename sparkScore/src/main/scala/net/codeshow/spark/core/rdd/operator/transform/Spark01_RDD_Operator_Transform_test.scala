package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //@todo 算子-map

    val rdd = sc.textFile("datas/apache.log")

    //长的字符串转换成短的字符串
    val mapRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }

}
