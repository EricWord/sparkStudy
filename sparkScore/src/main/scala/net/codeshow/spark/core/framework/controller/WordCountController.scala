package net.codeshow.spark.core.framework.controller

import net.codeshow.spark.core.framework.common.TController
import net.codeshow.spark.core.framework.service.WordCountService

class WordCountController extends TController{
  private val wordCountService = new WordCountService()

  def dispatch(): Unit = {
    val arr = wordCountService.dataAnalysis()
    arr.foreach(println)

  }

}
