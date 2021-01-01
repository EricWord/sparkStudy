package net.codeshow.spark.sql

object Spark06_SparkSQL_Test1 {


  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()


    //    准备数据
    //    todo 记得先创建数据库
    spark.sql("use codeshow")

spark.sql(
  """
    |""".stripMargin)

    spark.close()
  }


}
