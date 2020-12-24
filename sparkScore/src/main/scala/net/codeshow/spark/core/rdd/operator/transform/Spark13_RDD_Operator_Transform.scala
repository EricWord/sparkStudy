package net.codeshow.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //@todo 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //@todo 算子
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6), 2)
    //交集、并集和差集要求两个数据源数据类型保持一致
    //拉链操作两个数据源的类型可以不一致
    //交集
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    // 并集
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链
    //zip要求
    // 两个数据源的分区数要保持一致，否则报 Can't zip RDDs with unequal numbers of partitions
    //两个数据源的每个分区中的数据数量要保持一致，否则报 Can only zip RDDs with same number of elements in each partition
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


    sc.stop()
  }
}
