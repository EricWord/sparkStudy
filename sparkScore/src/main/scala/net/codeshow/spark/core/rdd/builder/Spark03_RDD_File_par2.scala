package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_par2 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD


    //如果数据源为多个文件，那么计算分区时是以文件为单位进行分区
    val rdd = sc.textFile("datas/word.txt", 2)
    rdd.saveAsTextFile("output")


    //@todo 关闭环境
    sc.stop()
  }
}
