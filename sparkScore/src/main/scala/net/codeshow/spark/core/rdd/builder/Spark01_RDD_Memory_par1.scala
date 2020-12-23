package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_par1 {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD
    //RDD的并行度 & 分区
    //makeRDD可以传递两个参数，第二个参数表示分区的数量
    //第二个参数如果不传递会使用默认值
    //scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //Spark在默认情况下，从配置对象中获取配置参数spark.default.parallelism
    //如果获取不到，则使用totalCores属性，这个属性取值为当前环境的最大核数
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4)

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")
    //@todo 关闭环境
    sc.stop()
  }
}
