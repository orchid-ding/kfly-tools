package top.kfly.hbase.source

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import top.kfly.common.Constants

/**
 * 自定义spark sql数据源
 *  {@link ReadSupport }支持读取操作
 */
class HBaseCustomSource extends DataSourceV2 with ReadSupport{

  override def createReader(options: DataSourceOptions): DataSourceReader = {
      val hbaseTableName = options.get(Constants.HBASE_TABlE_NAME).get()
      val sparkSqlTableSchema  = options.get(Constants.SPARK_SQL_TABlE_SCHEMA).get()
      val hbaseTableSchema = options.get(Constants.HBASE_TABLE_SCHEMA).get()
      new HBaseCustomDataSourceReader(hbaseTableName,sparkSqlTableSchema,hbaseTableSchema)
  }
}
