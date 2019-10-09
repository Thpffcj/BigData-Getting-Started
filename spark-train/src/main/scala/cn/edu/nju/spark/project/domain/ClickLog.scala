package cn.edu.nju.spark.project.domain

/**
  * Created by Thpffcj on 2018/1/15.
  * 清洗后的日志信息
  * @param ip  日志访问的ip地址
  * @param time  日志访问的时间
  * @param courseId  日志访问的实战课程编号
  * @param statusCode 日志访问的状态码
  * @param referrer  日志访问的referrer
  */
case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referrer:String)
