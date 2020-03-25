package top.kfly.common;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author dingchuangshi
 */
public class ConfigUtil {

    static Properties properties = new Properties();

    static {
        try {
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = ConfigUtil.class.getClassLoader()
                    .getResourceAsStream("config.properties");
            // 使用properties对象加载输入流
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 重置配置文件，传入相对路径resource下的文件
     * @param fileName
     */
    public static void resetProperties(String fileName){
        try {
            properties.clear();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = ConfigUtil.class.getClassLoader()
                    .getResourceAsStream(fileName);
            // 使用properties对象加载输入流
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 重置配置文件，传入相对路径resource下的文件
     * @param properties
     */
    public static void resetProperties(Properties properties){
        ConfigUtil.properties = properties;
    }

    /**
     * 获取key对应的value值
     *
     * @param key
     * @return
     */
    public static String getConfig(String key) {
        return (String) properties.get(key);
    }

    public static Properties getProperties() {
        return properties;
    }
}
