package net.codeshow.spark.sql

object Spark03_SparkSQL_UDAF1 {

  import org.apache.spark.sql.expressions.Aggregator


  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.{functions, SparkSession}
    //    TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 指定逻辑操作
    //  TODO  DataFrame

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF2()))
    spark.sql("select ageAvg(age) from user").show()

    //    TODO 关闭环境
    spark.close()
  }

  /*
  自定义聚合函数类：计算年龄的平均值

   */

  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF2 extends Aggregator[Long, Buff, Long] {

    import org.apache.spark.sql.{Encoder, Encoders}

    //    初始值或零值
    override def zero: Buff = {
      Buff(0, 0L)
    }

    //根据输入的数据更新缓冲区的数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
