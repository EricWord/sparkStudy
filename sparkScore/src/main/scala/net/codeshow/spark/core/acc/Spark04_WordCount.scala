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
    sc.register(wcAcc, "wordCountAcc")


    rdd.map((_, 1)).reduceByKey(_ + _)
    rdd.foreach(
      word => {
        //数据的累加
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)
    sc.stop()
  }

  /*
  自定义数据累加器:WordCount

   */
  class MyAccmulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty

    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccmulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)

    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }

    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap

    }
  }

}
