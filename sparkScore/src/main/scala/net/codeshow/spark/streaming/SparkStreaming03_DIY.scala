package net.codeshow.spark.streaming

object SparkStreaming03_DIY {

  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.receiver.Receiver

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val messageDS = ssc.receiverStream(new MyReceiver())
    messageDS.print()

    ssc.start()

    ssc.awaitTermination()
  }

  /*
  自定义数据采集器
  1. 继承Receiver，定义泛型,传递参数
  2.重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true

    override def onStart(): Unit = {

      new Thread(() => {
        while (flag) {
          import scala.util.Random
          val message = "采集的数据为:" + new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)

        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
