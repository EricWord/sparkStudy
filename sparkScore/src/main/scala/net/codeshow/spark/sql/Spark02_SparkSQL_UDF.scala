package net.codeshow.spark.sql

object Spark02_SparkSQL_UDF {
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
    spark.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })
    spark.sql("select age, prefixName(username) from user").show()

    //    TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
