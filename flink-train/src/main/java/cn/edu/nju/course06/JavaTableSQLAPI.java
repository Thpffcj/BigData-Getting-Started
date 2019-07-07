package cn.edu.nju.course06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by thpffcj on 2019-07-06.
 */
public class JavaTableSQLAPI {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String filePath = "file:///Users/thpffcj/Public/data/sales.csv";
        DataSet<Sales> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(Sales.class, "transactionId", "customerId", "itemId", "amountPaid");

        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales", sales);
        Table resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");

        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();
    }

    public static class Sales {
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double amountPaid;
    }
}
