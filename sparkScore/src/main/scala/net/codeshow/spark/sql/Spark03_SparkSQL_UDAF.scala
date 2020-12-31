package net.codeshow.spark.sql

object Spark03_SparkSQL_UDAF {

  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    //    TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 指定逻辑操作
    //  TODO  DataFrame

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg", new MyAvgUDAF)
    spark.sql("select ageAvg(age) from user").show()

    //    TODO 关闭环境
    spark.close()
  }

  /*
  自定义聚合函数类：计算年龄的平均值

   */

  class MyAvgUDAF extends UserDefinedAggregateFunction {

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.expressions.MutableAggregationBuffer
    import org.apache.spark.sql.types.{DataType, LongType, StructType}

    //    输入数据的结构
    override def inputSchema: StructType = {
      import org.apache.spark.sql.types
      import org.apache.spark.sql.types.LongType
      StructType(
        Array(
          types.StructField("age", LongType)

        )
      )

    }

    //缓冲区数据的结构
    override def bufferSchema: StructType = {
      import org.apache.spark.sql.types.{LongType, StructField}
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)

        )
      )
    }

    //函数计算结果的数据类型
    override def dataType: DataType = LongType

    //函数的稳定性
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    //根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    //计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
