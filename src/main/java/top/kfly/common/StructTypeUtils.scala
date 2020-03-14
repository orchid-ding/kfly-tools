package top.kfly.common

import org.apache.spark.sql.types.StructType

object StructTypeUtils {

  def filterStructSchema(structType:StructType):StructType={
    val structFields = structType.filter(value=>value.name!="goodsMoney")
    StructType.apply(structFields)
  }

}
