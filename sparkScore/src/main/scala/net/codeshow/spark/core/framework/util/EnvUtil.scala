package net.codeshow.spark.core.framework.util

object EnvUtil {

  import org.apache.spark.SparkContext

  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}
