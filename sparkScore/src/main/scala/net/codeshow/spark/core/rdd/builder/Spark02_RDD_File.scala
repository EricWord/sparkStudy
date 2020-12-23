package net.codeshow.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //@todo 创建RDD
    //从文件中创建RDD,将文件中的数据作为处理的数据源
    //路径默认是以当前环境的根路径为基准
    //可以写绝对路径，也可以写相对路径
    //    val rdd = sc.textFile("datas/1.txt")
    //除了可以像上面那样，指定文件的路径，也可以指定多个文件所在的目录
    //    val rdd = sc.textFile("datas")
    //路径还可以使用通配符
    //    val rdd = sc.textFile("datas/1*.txt")
    //path还可以是分布式文件系统路径:HDFS
    val rdd = sc.textFile("hdfs://Hadoop02:9000/test.txt")
    rdd.collect().foreach(println)
    //@todo 关闭环境
    sc.stop()
  }
}
