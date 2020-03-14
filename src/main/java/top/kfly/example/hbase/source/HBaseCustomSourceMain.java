package top.kfly.example.hbase.source;

import com.google.inject.internal.util.$SourceProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import top.kfly.common.Contains;

/**
 * @author dingchuangshi
 */
public class HBaseCustomSourceMain {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(HBaseCustomSourceMain.class.getName());
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
//        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> load = spark.read()
                .format("top.kfly.hbase.source.HBaseCustomSource")
                .option(Contains.HBASE_TABlE_NAME, "flink:kfly_orders")
                // ,f1:payFrom,f1:province,1:realTotalMoney
                .option(Contains.HBASE_TABLE_SCHEMA, " f1:userId , f1:goodId , f1:goodsMoney , f1:orderNo ")
                .option(Contains.SPARK_SQL_TABlE_SCHEMA, "userId String,goodId String,goodsMoney String,orderNo String")
                .load().filter("goodId > 1000").select("userId","goodId","orderNo");

        load.explain(true);

        load.show();
//        load.createOrReplaceTempView("orders");
//
//        spark.sql("select * from orders where goodId > 1000").show();

    }
}

