package net.codeshow.spark.streaming

object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "Hadoop02:9092,Hadoop03:9092,Hadoop04:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "codeshow_group",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("codeshow"), kafkaPara)
    )

    kafkaDataDS.map(_.value()).print()


    ssc.start()

    ssc.awaitTermination()
  }
}
