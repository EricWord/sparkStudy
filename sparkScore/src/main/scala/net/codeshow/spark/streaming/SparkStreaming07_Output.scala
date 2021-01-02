package net.codeshow.spark.streaming

object SparkStreaming07_Output {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))
    //    reduceByKeyAndWindow:当窗口的范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
    //    无需重复计算,提升性能
    val windowDS = wordToOne
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => {
          x + y
        },
        (x: Int, y: Int) => {
          x - y
        },
        Seconds(6), Seconds(6)

      )
    //    val wordToCount = windowDS.reduceByKey(_ + _)
    //    SparkStreaming如果没有输出操作，会提示错误
    //    wordToCount.print()

    //    foreachRDD不会出现时间戳
    windowDS.foreachRDD(
      rdd => {

      }
    )


    ssc.start()

    ssc.awaitTermination()
  }
}
