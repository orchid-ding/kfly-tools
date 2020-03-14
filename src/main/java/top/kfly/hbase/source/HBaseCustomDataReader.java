package top.kfly.hbase.source;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import top.kfly.common.JavaConversion;
import top.kfly.common.HBaseTools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseCustomDataReader implements DataReader<Row>

{
    private String hbaseTableName;
    private String hbaseTableSchema;
    private Connection hbaseConnection;
    private Iterator<Result> iterable;
    private List<String> requiredSchemaList;

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
            String[] splitFamilies = hbaseTableSchema.split(",");
            Scan scan = new Scan();
            for(String families : splitFamilies){
                String[] splitColumns = families.split(":");
                String cloumn = splitColumns[1].trim();
                // 列裁剪字段
                if(requiredSchemaList.contains(cloumn)){
                    System.out.println(cloumn);
                    scan.addColumn(splitColumns[0].trim().getBytes(),cloumn.getBytes());
                }
            }
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return scanner.iterator();
    }

    public HBaseCustomDataReader(String hbaseTableName, String hbaseTableSchema,List<String> requiredSchemaList){
        this.hbaseTableName = hbaseTableName;
        this.hbaseTableSchema = hbaseTableSchema;
        this.requiredSchemaList = requiredSchemaList;
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
        String[] splitFamilies = hbaseTableSchema.split(",");
        for(String families : splitFamilies){
            String[] splitColumns = families.split(":");
            String column =  splitColumns[1].trim();
            if(requiredSchemaList.contains(column)){
                resultLists.add(Bytes.toString(result.getValue(splitColumns[0].trim().getBytes(), column.getBytes())));
            }
        }
        return JavaConversion.asScala(resultLists);
    }

    @Override
    public void close() throws IOException {
        hbaseConnection.close();
    }
}
