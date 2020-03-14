package top.kfly.hbase.source;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import top.kfly.common.Contains;

/**
 * @author dingchuangshi
 */
public class HBaseCustomSource implements DataSourceV2 , ReadSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
        String hbaseTableName = dataSourceOptions.get(Contains.HBASE_TABlE_NAME).get();
        String sparkSqlTableSchema  = dataSourceOptions.get(Contains.SPARK_SQL_TABlE_SCHEMA).get();
        String hbaseTableSchema = dataSourceOptions.get(Contains.HBASE_TABLE_SCHEMA).get();
        return new HBaseCustomDataSourceReader(hbaseTableName,sparkSqlTableSchema,hbaseTableSchema);
    }
}
