package net.codeshow.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello", "spark", "hello"))

    //累加器
    //    1.创建累加器对象
    //    2.向Spark进行注册
    val wcAcc = new MyAccmulator()
    sc.register(wcAcc)


    rdd.map((_, 1)).reduceByKey(_ + _)
    rdd.foreach(
      word => {
        //数据的累加
        wcAcc.add(word)
      }
    )


    sc.stop()
  }

  /*
  自定义数据累加器:WordCount

   */
  class MyAccmulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    override def isZero: Boolean = ???

    override def copy(): AccumulatorV2[Nothing, Nothing] = ???

    override def reset(): Unit = ???

    override def add(v: Nothing): Unit = ???

    override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit = ???

    override def value: Nothing = ???
  }

}
