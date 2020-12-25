package net.codeshow.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("spark://Hadoop02:7077").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordCount9(sc)


    sc.stop()
  }

  def wordCount1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group = words.groupBy(word => word)
    val wordCnt = group.mapValues(iter => iter.size)
  }

  def wordCount2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))

    val group = wordOne.groupByKey()
    val wordCnt = group.mapValues(iter => iter.size)
  }

  def wordCount3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))

    val wordCnt = wordOne.reduceByKey(_ + _)

  }

  def wordCount4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))

    val wordCnt = wordOne.aggregateByKey(0)(_ + _, _ + _)

  }

  def wordCount5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))

    val wordCnt = wordOne.foldByKey(0)(_ + _)

  }

  def wordCount6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))

    val wordCnt = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )

  }


  def wordCount7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val stringToLong = wordOne.countByKey()
  }

  def wordCount8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val stringToLong = words.countByValue()
  }

  def wordCount9(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCnt = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCnt)
  }

}
