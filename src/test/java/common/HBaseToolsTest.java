package common;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import top.kfly.common.ConfigUtil;
import top.kfly.common.HBaseTools;

public class HBaseToolsTest {

    @Test
    public void testHBaseconn(){

        Connection conn = HBaseTools.getConnection();

        HBaseTools.closeConn();
        ConfigUtil.resetProperties("config1.properties");
        conn = HBaseTools.getConnection();
    }
}
