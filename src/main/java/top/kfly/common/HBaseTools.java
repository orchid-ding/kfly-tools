package top.kfly.common;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class HBaseTools {

    private static Connection connection = null;

    public static Configuration getHBaseConfiguration(){
        Configuration hconf = HBaseConfiguration.create();
        hconf.set(Constants.HBASE_ZOOKEEPER_QUORUM, ConfigUtil.getConfig(Constants.HBASE_ZOOKEEPER_QUORUM));
        hconf.set(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, ConfigUtil.getConfig(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT));
        hconf.setInt(Constants.HBASE_CLIENT_OPERATION_TIMEOUT, Integer.parseInt(ConfigUtil.getConfig(Constants.HBASE_CLIENT_OPERATION_TIMEOUT)));
        hconf.set(Constants.ZOOKEEPER_ZNODE_PARENT,ConfigUtil.getConfig(Constants.ZOOKEEPER_ZNODE_PARENT));
        return hconf;
    }

    public static Connection getConnection(){
        if(connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(getHBaseConfiguration());
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("HBase connect failed");
            }
        }
        return connection;
    }

    /**
     * 创建表
     * @param tableNameString hbase table name
     * @param columnFamilies 列镞
     * @param splitKey hbase预分区
     * @throws IOException
     */
    public static void createTable(Connection connection, String tableNameString, List<String> columnFamilies,byte[][] splitKey) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        HTableDescriptor table = new HTableDescriptor(tableName);
        columnFamilies.forEach(columnFamily->{
            HColumnDescriptor family = new HColumnDescriptor(columnFamily);
            table.addFamily(family);
        });
        //判断表是否已经存在
        if (!admin.tableExists(tableName)) {
            admin.createTable(table,splitKey);
        }else{
            //如果表已经存在了，判断列族是否存在
            Table tableTemp = connection.getTable(TableName.valueOf(tableNameString));
            HTableDescriptor tableDescriptor = tableTemp.getTableDescriptor();
            HColumnDescriptor[] columnFamiliesTemp = tableDescriptor.getColumnFamilies();
            List<String> columnFamilyList = new ArrayList<>();
            for (HColumnDescriptor hColumnDescriptor : columnFamiliesTemp) {
                columnFamilyList.add(hColumnDescriptor.getNameAsString());
            }
            for (String family : columnFamilies) {
                if(!columnFamilyList.contains(family)){
                    admin.modifyColumn(tableName,new HColumnDescriptor(family));
                }
            }

        }
        admin.close();
        connection.close();
    }

    /**
     * 获取HBase预分区
     * @param regionNum
     * @return
     */
    public byte[][] getSplitKey(int regionNum){
        byte[][] byteNum = new byte[regionNum][];
        for(int i =0;i<regionNum;i++){
            String leftPad = StringUtils.leftPad(i+"",4,"0");
            byteNum[i] = Bytes.toBytes(leftPad + "|");
        }
        return byteNum;
    }



}
