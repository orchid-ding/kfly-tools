package top.kfly.hbase.source;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;

import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataReadFactory implements DataReaderFactory<Row> {

    private String hbaseTableName;
    private  String hbaseTableSchame;
    private List<String> requiredSchemaList;

    public HBaseCustomDataReadFactory(String hbaseTableName, String hbaseTableSchema, List<String> requiredSchemaList) {
        this.hbaseTableName = hbaseTableName;
        this.hbaseTableSchame = hbaseTableSchema;
        this.requiredSchemaList = requiredSchemaList;
    }

    @Override
    public DataReader<Row> createDataReader() {
        return new HBaseCustomDataReader(hbaseTableName,hbaseTableSchame,requiredSchemaList);
    }
}
