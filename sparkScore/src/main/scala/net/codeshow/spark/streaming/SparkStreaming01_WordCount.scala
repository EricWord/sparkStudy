package net.codeshow.spark.streaming

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}

    //    TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //    第一个参数表示环境配置
    //    第二个参数表示批量处理的周期(采集周期)
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //    TODO 逻辑处理
    //    获取端口数据
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val wordToCount = wordToOne.reduceByKey(_ + _)
    wordToCount.print()


    //由于SparkStreaming是一个长期执行的任务，所以不能关闭
    //    如果main方法执行完毕，应用程序也会自动结束，所以不能让main方法执行完毕
    //    ssc.stop()
    //    1.启动采集器
    ssc.start()
    //    2.等待采集器关闭
    ssc.awaitTermination()
  }

}
