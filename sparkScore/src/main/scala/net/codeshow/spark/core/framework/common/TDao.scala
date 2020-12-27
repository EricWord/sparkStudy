package net.codeshow.spark.core.framework.common

trait TDao {
  def readFile(path: String) = {
    import net.codeshow.spark.core.framework.util.EnvUtil
    EnvUtil.take().textFile(path)
  }


}
