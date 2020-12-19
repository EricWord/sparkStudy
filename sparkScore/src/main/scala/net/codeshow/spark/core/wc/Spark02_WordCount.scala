package net.codeshow.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //@todo 建立和Spark框架的连接

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //@todo 执行业务操作

    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )


    val wordGroup = wordToOne.groupBy(
      t => t._1
    )
    val wordCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    //5.将转换结果采集到控制台打印出来
    val arr = wordCount.collect()
    arr.foreach(println)

    //@todo 关闭连接
    sc.stop()


  }

}
