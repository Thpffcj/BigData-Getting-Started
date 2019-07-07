package cn.edu.nju.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * Created by thpffcj on 2019-07-06.
  */
object TableSQLAPI {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val filePath = "file:///Users/thpffcj/Public/data/sales.csv"
    // 已经拿到DataSet
    val csv = env.readCsvFile[SalesLog](filePath, ignoreFirstLine = true)

    // DataSet => Table
    val salesTable = tableEnv.fromDataSet(csv)
    // Table => table
    tableEnv.registerTable("sales", salesTable)

    // sql
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()
  }

  case class SalesLog(transactionId: String, customerId: String, itemId: String, amountPaid: Double)
}
