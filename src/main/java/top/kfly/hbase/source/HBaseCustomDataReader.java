package top.kfly.hbase.source;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import top.kfly.common.JavaConversion;
import top.kfly.common.HBaseTools;
import top.kfly.common.StructTypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataReader implements DataReader<Row>
        , SupportsPushDownFilters, SupportsPushDownRequiredColumns
{
    private String hbaseTableName;
    private String hbaseTableSchame;
    private Connection hbaseConnection;
    private List<Filter> supportFilters = new ArrayList<>();
    private StructType requiredSchema;
    private Iterator<Result> iterable;

    /**
     * load hbase data
     * @return
     * @throws IOException
     */
    private Iterator<Result> getIterator() {
        hbaseConnection = HBaseTools.getConnection();
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = hbaseConnection.getTable(TableName.valueOf(hbaseTableName.trim()));
            String[] splitFamilies = hbaseTableSchame.split(",");
            Scan scan = new Scan();
            for(String families : splitFamilies){
                String[] splitColumns = families.split(":");
                scan.addColumn(splitColumns[0].trim().getBytes(),splitColumns[1].trim().getBytes());
            }
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return scanner.iterator();
    }

    public HBaseCustomDataReader(String hbaseTableName, String hbaseTableSchame){
        this.hbaseTableName = hbaseTableName;
        this.hbaseTableSchame = hbaseTableSchame;
        this.iterable =  getIterator();
    }

    @Override
    public boolean next() throws IOException {
        return iterable.hasNext();
    }

    @Override
    public Row get() {
        Result result = iterable.next();
        List<Object> resultLists = new ArrayList<>();
        String[] splitFamilies = hbaseTableSchame.split(",");
        for(String families : splitFamilies){
            String[] splitColumns = families.split(":");
            resultLists.add(Bytes.toString(result.getValue(splitColumns[0].trim().getBytes(), splitColumns[1].trim().getBytes())));
        }
        return JavaConversion.asScala(resultLists);
    }

    @Override
    public void close() throws IOException {
        hbaseConnection.close();
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> unSupportFilters = new ArrayList<>();
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
        return (Filter[]) unSupportFilters.toArray();
    }

    @Override
    public Filter[] pushedFilters() {
        return (Filter[]) supportFilters.toArray();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public StructType readSchema() {
        return requiredSchema;
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        List<DataReaderFactory<Row>> dataReadFactories = new ArrayList<>();
        dataReadFactories.add(new HBaseCustomDataReadFactory(hbaseTableName,hbaseTableSchame));
        return dataReadFactories;
    }
}
