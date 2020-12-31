package net.codeshow.spark.sql

object Spark06_SparkSQL_Test {


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
        |CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data2/user_visit_action.txt' into table codeshow.user_visit_action""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data2/product_info.txt' into table codeshow.product_info
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data2/city_info.txt' into table codeshow.city_info""".stripMargin)


    spark.sql("select * from city_info").show()
    spark.close()
  }


}
