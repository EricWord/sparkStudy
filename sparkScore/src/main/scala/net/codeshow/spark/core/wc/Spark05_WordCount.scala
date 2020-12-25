package net.codeshow.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }


}
