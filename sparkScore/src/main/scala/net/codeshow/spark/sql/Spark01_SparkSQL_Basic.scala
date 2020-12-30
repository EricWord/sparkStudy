package net.codeshow.spark.sql

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    //    TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 指定逻辑操作
    //  TODO  DataFrame

    val df = spark.read.json("datas/user.json")
    df.show()

    //  TODO  DataFrame => SQL
    //    df.createOrReplaceTempView("user")
    //    spark.sql("select * from user").show()
    //    spark.sql("select username,age from user").show()
    //    spark.sql("select avg(age) from user").show()

    //  TODO  DataFrame => DSL
    //    在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    //import spark.implicits._
    //    df.select("age", "username").show
    //    df.select($"age" + 1).show
    //    df.select('age + 1).show


    // TODO   DataSet
    //    DataFrame 其实是特定泛型的DataSet
    //    import spark.implicits._
    //    var se = Seq(1, 2, 3, 4)
    //    val ds = se.toDS
    //    ds.show()

    //  TODO  RDD <=> DataFrame
    import spark.implicits._
    val rdd1 = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df2 = rdd1.toDF("id", "name", "age")
    val rdd2 = df2.rdd

    //  TODO  DataFrame <=> DataSet

    val ds2 = df.as[User]
    val df3 = ds2.toDF()
    //  TODO  RDD <=> DataSet
    val ds3 = rdd1.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd4 = ds3.rdd


    //    TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
