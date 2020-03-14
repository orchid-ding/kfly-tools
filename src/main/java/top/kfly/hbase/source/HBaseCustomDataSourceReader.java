package top.kfly.hbase.source;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataSourceReader implements DataSourceReader {

    private String hbaseTableName;
    private  String sparkSqlTableSchema;
    private String hbaseTableSchame;

    public HBaseCustomDataSourceReader(){}

    public HBaseCustomDataSourceReader(String hbaseTableName, String sparkSqlTableSchema, String hbaseTableSchema) {
        this.hbaseTableName = hbaseTableName;
        this.sparkSqlTableSchema = sparkSqlTableSchema;
        this.hbaseTableSchame = hbaseTableSchema;
    }

    @Override
    public StructType readSchema() {
        return StructType.fromDDL(sparkSqlTableSchema);
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        List<DataReaderFactory<Row>> dataReadFactories = new ArrayList<>();
        dataReadFactories.add(new HBaseCustomDataReadFactory(hbaseTableName,hbaseTableSchame));
        return dataReadFactories;
    }
}
