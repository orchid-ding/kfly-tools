package top.kfly.hbase.source

import java.io.IOException

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter, ValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.StructType
import top.kfly.common.HBaseTools


class HBaseCustomDataReader(hbaseTableName: String, hbaseTableSchema: String,sparkSqlTableSchema:String, supportsFilters: Array[Filter], requiredSchema: StructType)
        extends DataReader[Row]{

  var hbaseConnection : Connection = null

  val datas:Iterator[Result] = getIterator

  /**
   * load hbase data
   * @return
   * @throws IOException
   */
  def getIterator: Iterator[Result] = {
    hbaseConnection = HBaseTools.getConnection
    var table: Table = null
    var scanner: ResultScanner = null
    val requiredSchemaList = requiredSchema.map(x=>x.name)
    try {
      table = hbaseConnection.getTable(TableName.valueOf(hbaseTableName.trim))
      val scan: Scan = new Scan
      fullScanByColumnsAndFilters(scan,requiredSchemaList)
      scanner = table.getScanner(scan)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    import scala.collection.JavaConverters._
    scanner.iterator.asScala
  }

  override def next(): Boolean = {
    datas.hasNext
  }

  override def get(): Row = {
    val result = datas.next()
    var sparkSqlTuples = sparkSqlTableSchema.split(",").map(x => {
      val strings = x.split(" ")
      (strings(0).trim, strings(1).trim.toLowerCase)
    })

    if(requiredSchema.size > 0){
      val requiredSchemaList = requiredSchema.map(x=>x.name)
      sparkSqlTuples = sparkSqlTuples.filter(x=>requiredSchemaList.contains(x._1))
    }

    val tableSchemaMap = hbaseTableSchema.split(",").map(x=>{
      val strings = x.split(":")
      (strings(1).trim,strings(0).trim)
    }).toMap
    val array = sparkSqlTuples.map {
      case tuple if (tuple._2.equals("int")) => Bytes.toString(result.getValue(tableSchemaMap.get(tuple._1).get.getBytes, tuple._1.getBytes)).toInt
      case tuple => Bytes.toString(result.getValue(tableSchemaMap.get(tuple._1).get.getBytes, tuple._1.getBytes))
    }

    Row.fromSeq(array)
  }

  override def close(): Unit = hbaseConnection.close()

  /**
   * 填充columns and filters
   * @param scan
   * @param requiredSchemaList
   * @return
   */
  def fullScanByColumnsAndFilters(scan: Scan, requiredSchemaList: Seq[String])={
    fullScanByColumns(scan,requiredSchemaList)
    fullScanByFilter(scan)
  }

  /**
   * 填充scan，拼接要查询的列
   * @param scan
   * @param requiredSchemaList
   * @return
   */
  def fullScanByColumns(scan:Scan,requiredSchemaList:Seq[String]) = {
    // 1. 拼接查询所需要的列
    var hbaseTableSchemaTuples = hbaseTableSchema.split(",").map(x => {
      val tupleString = x.split(":")
      (tupleString(0).trim, tupleString(1).trim)
    });
    if(requiredSchemaList.size > 0){
      hbaseTableSchemaTuples = hbaseTableSchemaTuples.filter(x => requiredSchemaList.contains(x._2))
    }
    hbaseTableSchemaTuples
      .map(tuple=>{
        scan.addColumn(tuple._1.trim.getBytes,tuple._2.getBytes)
      })
  }


  /**
   * 拼接所需要的filter
   * @param scan
   * @return
   */
  def fullScanByFilter(scan: Scan) = {
    val tableSchemaMap = hbaseTableSchema.split(",").map(x => {
                    val tupleString = x.split(":")
                    (tupleString(1).trim, tupleString(0).trim)
          }).toMap

    // 2. 拼接所需要的filter supportFilters
    val filterList = new FilterList()
    supportsFilters.foreach{
      case filter: EqualTo => {
        filterList.addFilter(new SingleColumnValueFilter(tableSchemaMap.get(filter.attribute).get.getBytes, filter.attribute.getBytes, CompareOp.EQUAL, filter.value.toString.getBytes))
      }
      case filter: GreaterThan =>{
        filterList.addFilter(new SingleColumnValueFilter(tableSchemaMap.get(filter.attribute).get.getBytes, filter.attribute.getBytes, CompareOp.GREATER, filter.value.toString.getBytes))
      }
      case filter: GreaterThanOrEqual =>{
        filterList.addFilter(new SingleColumnValueFilter(tableSchemaMap.get(filter.attribute).get.getBytes, filter.attribute.getBytes, CompareOp.GREATER_OR_EQUAL, filter.value.toString.getBytes))
      }
      case filter: LessThan =>{
        filterList.addFilter(new SingleColumnValueFilter(tableSchemaMap.get(filter.attribute).get.getBytes, filter.attribute.getBytes, CompareOp.LESS, filter.value.toString.getBytes))
      }
      case filter: LessThanOrEqual =>{
        filterList.addFilter(new SingleColumnValueFilter(tableSchemaMap.get(filter.attribute).get.getBytes, filter.attribute.getBytes, CompareOp.LESS_OR_EQUAL, filter.value.toString.getBytes))
      }
    }
    if(filterList.getFilters.size() > 0){
      scan.setFilter(filterList)
    }
  }
}

