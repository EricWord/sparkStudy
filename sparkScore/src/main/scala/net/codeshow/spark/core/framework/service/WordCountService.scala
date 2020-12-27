package net.codeshow.spark.core.framework.service

import net.codeshow.spark.core.framework.common.TService
import net.codeshow.spark.core.framework.dao.WordCountDao

class WordCountService extends TService {
  private val wordCountDao = new WordCountDao()

  def dataAnalysis() = {

    val lines = wordCountDao.readFile("datas/11.txt")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup = wordToOne.groupBy(
      t => t._1
    )
    val wordCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    wordCount.collect()
  }
}
