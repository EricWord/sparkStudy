package net.codeshow.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search = new Search("h")
    //    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)
  }

  //查询对象
  //类的构造参数其实是类的属性
  //构造参数需要进行闭包就检测，其实就等同于类进行闭包检测
  class Search(query: String) {

    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }

}
