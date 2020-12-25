package net.codeshow.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("datas/11.txt")
    println(lines.toDebugString)
    println("**************")
    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("**************")
    val wordGroup = words.groupBy(word => word)
    val wordCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val arr = wordCount.collect()
    arr.foreach(println)
    sc.stop()

  }

}
