package cn.edu.nju.log

import com.ggstar.util.ip.IpHelper

/**
  * Created by Thpffcj on 2018/5/7.
  * IP解析工具类
  */
object IpUtils {

  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]) {
    println(getCity("218.197.153.150"))
  }
}
