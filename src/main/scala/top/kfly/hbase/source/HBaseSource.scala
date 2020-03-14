package top.kfly.hbase.source

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import top.kfly.common.Contains

class HBaseSource extends DataSourceV2 with ReadSupport{
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    val hbaseTableName = dataSourceOptions.get(Contains.HBASE_TABlE_NAME).get
    val sparkSqlTableSchema = dataSourceOptions.get(Contains.SPARK_SQL_TABlE_SCHEMA).get
    val hbaseTableSchema = dataSourceOptions.get(Contains.HBASE_TABLE_SCHEMA).get

    new HBaseDataSourceReader(hbaseTableName,sparkSqlTableSchema,hbaseTableSchema)
  }
}
