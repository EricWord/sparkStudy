package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD
    //从内存中创建RDD,将内存中的集合的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)
    //parallelize : 并行
    //    val rdd = sc.parallelize(seq)
    //上行代码的等价写法
    //makeRDD 在底层实现时，还是调用的parallelize
    val rdd = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    //@todo 关闭环境
    sc.stop()
  }
}
