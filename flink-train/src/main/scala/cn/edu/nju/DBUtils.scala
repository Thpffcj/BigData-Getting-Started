package cn.edu.nju

import scala.util.Random

/**
  * Created by thpffcj on 2019-07-02.
  */
object DBUtils {

  def getConnection()= {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection: String): Unit = {

  }
}
