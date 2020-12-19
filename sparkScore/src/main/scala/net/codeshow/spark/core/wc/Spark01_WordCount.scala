package net.codeshow.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //@todo 建立和Spark框架的连接

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //@todo 执行业务操作

    //1.读取文件，获取一行一行的数据
    val lines = sc.textFile("datas")
    //2.将一行数据进行拆分，形成一个一个的单词(分词)
    //扁平化:将整体拆分成个体的操作
    //按照空格进行分割
    val words = lines.flatMap(_.split(" "))
    //3.将数据根据单词进行分组，便于统计
    val wordGroup = words.groupBy(word => word)
    //4.对分组后的数据进行转换
    //结构的转换一般都是使用map
    val wordCount = wordGroup.map {
      //模式匹配
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换结果采集到控制台打印出来
    val arr = wordCount.collect()
    arr.foreach(println)

    //@todo 关闭连接
    sc.stop()


  }

}
