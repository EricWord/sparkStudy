package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_par1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD
    //@todo 数据分区的分配
    //1.数据以行为单位进行读取
    //Spark读取文件，采用的是Hadoop的方式读取，所以一行一行读取，和字节数没有关系
    //2.数据读取时，以偏移量为单位，偏移量不会被重复读取
    //3.数据分区的偏移量范围的计算
    val rdd = sc.textFile("datas/1.txt", 2)
    rdd.saveAsTextFile("output")


    //@todo 关闭环境
    sc.stop()
  }
}
