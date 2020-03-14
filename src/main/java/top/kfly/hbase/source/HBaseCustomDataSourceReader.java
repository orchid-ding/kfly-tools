package top.kfly.hbase.source;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.optimizer.CombineFilters;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataSourceReader implements DataSourceReader , SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private String hbaseTableName;
    private  String sparkSqlTableSchema;
    private String hbaseTableSchame;

    private List<Filter> supportFilters = new ArrayList<Filter>();
    private StructType requiredSchema;

    public HBaseCustomDataSourceReader(){}

    public HBaseCustomDataSourceReader(String hbaseTableName, String sparkSqlTableSchema, String hbaseTableSchema) {
        this.hbaseTableName = hbaseTableName;
        this.sparkSqlTableSchema = sparkSqlTableSchema;
        this.hbaseTableSchame = hbaseTableSchema;
    }

    @Override
    public StructType readSchema() {
        if(requiredSchema != null) {
            return requiredSchema;
        }
        return StructType.fromDDL(sparkSqlTableSchema);
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        List<DataReaderFactory<Row>> dataReadFactories = new ArrayList<>();
        Iterator<StructField> iterator = requiredSchema.iterator();
        List<String> requiredSchemaList = new ArrayList<>();
        while (iterator.hasNext()){
            requiredSchemaList.add(iterator.next().name().trim());
        }
        dataReadFactories.add(new HBaseCustomDataReadFactory(hbaseTableName,hbaseTableSchame,requiredSchemaList));
        return dataReadFactories;
    }


    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> unSupportFilters = new ArrayList<Filter>();
        if(filters.length > 0){
            for(Filter filter : filters){
                boolean isSupportFilter = filter instanceof EqualTo || filter instanceof GreaterThan || filter instanceof IsNotNull;

                if(isSupportFilter){
                    supportFilters.add(filter);
                }else {
                    unSupportFilters.add(filter);
                }
            }
        }
        return unSupportFilters.toArray(new Filter[unSupportFilters.size()]);
    }

    @Override
    public Filter[] pushedFilters() {
        return supportFilters.toArray(new Filter[supportFilters.size()]);
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public String toString() {
        return "HBaseCustomDataSourceReader{" +
                "hbaseTableName='" + hbaseTableName + '\'' +
                ", sparkSqlTableSchema='" + sparkSqlTableSchema + '\'' +
                ", hbaseTableSchame='" + hbaseTableSchame + '\'' +
                ", supportFilters=" + supportFilters +
                ", requiredSchema=" + requiredSchema +
                '}';
    }
}
