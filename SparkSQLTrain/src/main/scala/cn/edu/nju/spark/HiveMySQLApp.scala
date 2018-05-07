package cn.edu.nju.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Thpffcj on 2018/5/4.
  * 使用外部数据源综合查询Hive和MySQL的表数据
  * 不能直接运行，需要打包到服务器运行
  */
object HiveMySQLApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
//      .appName("HiveMySQLApp")
//      .master("local[2]")
      .getOrCreate()

    // 加载Hive表数据
    val hiveDF = spark.table("emp")

    // 加载MySQL表数据
    val mysqlDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "spark.DEPT")
      .option("user", "root")
      .option("password", "000000")
      .option("driver", "com.mysql.jdbc.Driver").load()

    // JOIN
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show

    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),
      mysqlDF.col("deptno"), mysqlDF.col("dname")).show

    spark.stop()
  }

}
