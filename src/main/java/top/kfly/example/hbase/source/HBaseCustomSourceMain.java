package top.kfly.example.hbase.source;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import top.kfly.common.ConfigUtil;
import top.kfly.common.Constants;
import top.kfly.common.HBaseTools;

/**
 * @author dingchuangshi
 */
public class HBaseCustomSourceMain {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(HBaseCustomSourceMain.class.getName());
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        // 数据源1
        Dataset<Row> load = spark.read()
                .format("top.kfly.hbase.source.HBaseCustomSource")
                .option(Constants.HBASE_TABlE_NAME, "flink:kfly_orders")
                .option(Constants.HBASE_TABLE_SCHEMA, " f1:userId,f1:goodsMoney,f1:orderNo,f1:goodId")
                .option(Constants.SPARK_SQL_TABlE_SCHEMA, "userId String,goodsMoney String,orderNo String,goodId int")
                .load()
                .select("goodsMoney","orderNo","goodId")
                .filter("goodId <= 16");

        ConfigUtil.resetProperties("config1.properties");
        HBaseTools.closeConn();

        // 数据源2
        Dataset<Row> load2 = spark.read()
                .format("top.kfly.hbase.source.HBaseCustomSource")
                .option(Constants.HBASE_TABlE_NAME, "flink:kfly_orders")
                .option(Constants.HBASE_TABLE_SCHEMA, " f1:userId,f1:goodsMoney,f1:orderNo,f1:goodId")
                .option(Constants.SPARK_SQL_TABlE_SCHEMA, "userId String,goodsMoney String,orderNo String,goodId int")
                .load();

        load.explain(true);
        load.show();

    }
}

