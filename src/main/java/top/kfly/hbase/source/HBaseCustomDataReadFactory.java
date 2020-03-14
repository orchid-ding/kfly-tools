package top.kfly.hbase.source;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataReadFactory implements DataReaderFactory<Row> {

    private String hbaseTableName;
    private  String hbaseTableSchame;

    public HBaseCustomDataReadFactory(String hbaseTableName, String hbaseTableSchame) {
        this.hbaseTableName = hbaseTableName;
        this.hbaseTableSchame = hbaseTableSchame;
    }

    @Override
    public DataReader<Row> createDataReader() {
        return new HBaseCustomDataReader(hbaseTableName,hbaseTableSchame);
    }
}
