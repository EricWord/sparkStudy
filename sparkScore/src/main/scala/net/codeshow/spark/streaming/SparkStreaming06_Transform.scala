package net.codeshow.spark.streaming

object SparkStreaming06_Transform {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)
//   transform可以将底层RDD获取到后进行操作
   val newDS = lines.transform(
     rdd => {
       rdd.map(
         str => {
           str
         }
       )
     }
   )
    val newDS1 = lines.map(
      data => {
        data
      }
    )



    ssc.start()

    ssc.awaitTermination()
  }
}
