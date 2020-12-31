package net.codeshow.spark.sql

object Spark03_SparkSQL_UDAF2 {

  import org.apache.spark.sql.expressions.Aggregator


  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    //    TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 指定逻辑操作
    //  TODO  DataFrame
    import spark.implicits._
    val df = spark.read.json("datas/user.json")
    //    早期版本中，spark不能在sql中使用强类型UDAF操作
    //    早期的UDAF强类型聚合函数使用DSL语法操作
    val ds = df.as[User]
    //    将UDAF函数转换为查询的列对象
    val udafCol = new MyAvgUDAF3().toColumn
    ds.select(udafCol).show()

    //    TODO 关闭环境
    spark.close()
  }

  /*
  自定义聚合函数类：计算年龄的平均值

   */

  case class User(username: String, age: Long)

  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF3 extends Aggregator[User, Buff, Long] {

    import org.apache.spark.sql.{Encoder, Encoders}

    //    初始值或零值
    override def zero: Buff = {
      Buff(0, 0L)
    }

    //根据输入的数据更新缓冲区的数据
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
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
