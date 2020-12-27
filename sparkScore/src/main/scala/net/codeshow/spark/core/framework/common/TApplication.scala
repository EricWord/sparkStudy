package net.codeshow.spark.core.framework.common

import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master: String = "local[*]", app: String = "WordCount")(op: => Unit): Unit = {
    import net.codeshow.spark.core.framework.util.EnvUtil
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    //@todo 执行业务操作

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    //@todo 关闭连接
    sc.stop()
    EnvUtil.clear()
  }

}
