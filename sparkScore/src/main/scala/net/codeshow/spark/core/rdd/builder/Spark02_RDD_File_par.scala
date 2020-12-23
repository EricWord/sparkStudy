package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_par {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD
    //textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    //minPartitions 最小分区数量 math.min(defaultParallelism, 2)
    //如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    //Spark读取文件，底层其实使用的就是Hadoop的读取方式
    //分区数量的计算方式
    //
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")


    //@todo 关闭环境
    sc.stop()
  }
}
