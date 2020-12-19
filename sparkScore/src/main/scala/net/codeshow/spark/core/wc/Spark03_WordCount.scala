package net.codeshow.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //@todo 建立和Spark框架的连接

    val sparkConf = new SparkConf().setMaster("spark://Hadoop02:7077").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //@todo 执行业务操作

    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )

    //Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    //reduceByKey:相同的key的数据可以对value进行聚合
    val wordCount = wordToOne.reduceByKey(_ + _)
    //5.将转换结果采集到控制台打印出来
    val arr = wordCount.collect()
    arr.foreach(println)

    //@todo 关闭连接
    sc.stop()
  }
}
