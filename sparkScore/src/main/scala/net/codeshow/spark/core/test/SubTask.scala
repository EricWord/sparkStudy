package net.codeshow.spark.core.test

class SubTask extends Serializable {

  var datas: List[Int] = _
  //任务虽然不同，但是逻辑相同
  var logic: (Int) => Int = _

  //计算
  def compute() = {
    datas.map(logic)

  }

}
