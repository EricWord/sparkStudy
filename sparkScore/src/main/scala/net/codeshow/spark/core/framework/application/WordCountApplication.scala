package net.codeshow.spark.core.framework.application

import net.codeshow.spark.core.framework.common.TApplication
import net.codeshow.spark.core.framework.controller.WordCountController

object WordCountApplication extends App with TApplication {
  start("local[*]", "WordCount") {
    val controller = new WordCountController()
    controller.dispatch()
  }
}
