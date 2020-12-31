package net.codeshow.spark.sql

object Spark05_SparkSQL_Hive {


  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    //    TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //    import spark.implicits._
    //    TODO 使用SparkSQl连接外置的Hive

    //1.拷贝hive-site.xml到classpath下
    //    2.启用hive的支持
    //    3.增加对应的依赖关系(包含MySQL的驱动)
    spark.sql("show tables").show()


    //    TODO 关闭环境
    spark.close()
  }


}
