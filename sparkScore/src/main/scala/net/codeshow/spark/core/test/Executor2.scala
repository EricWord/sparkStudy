package net.codeshow.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor2 {
  def main(args: Array[String]): Unit = {

    //启动服务器，接收数据
    val server = new ServerSocket(8888)
    println("服务器启动，等待接收数据...")

    //等待客户端的连接
    val client = server.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[SubTask]
    val res = task.compute()
    println("计算节点[8888]计算的结果为" + res)
    objIn.close()
    client.close()
    server.close()

  }

}
