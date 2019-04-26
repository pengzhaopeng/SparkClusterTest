package cn.pengzhaopeng.spark.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Properties props;

    static {
        String fileName = "conf.properties";// 配置文件名
        props = new Properties();
        try {
            props.load(new InputStreamReader(PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName), "UTF-8"));
        } catch (IOException e) {
            logger.error("配置文件读取异常", e);
        }
    }

    public static String getString(String key) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return value.trim();
    }

    public static int getInt(String key) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return -1;
        }
        return Integer.parseInt(value.trim());
    }

    public static long getLong(String key) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return -1;
        }
        return Long.parseLong(value.trim());
    }

    public static boolean getBoolean(String key) {
        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            return false;
        }
        return true;
    }

    public static String getString(String key, String defaultValue) {

        String value = props.getProperty(key.trim());
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value.trim();
    }

    /**
     * 写入Properties信息
     *
     * @param filePath 写入的属性文件
     * @param pKey     属性名称
     * @param pValue   属性值
     */
    public final static void WriteProperties(String filePath, String pKey, String pValue) throws IOException {
        Properties props = new Properties();

        props.load(new FileInputStream(filePath));
        // 调用 Hashtable 的方法 put，使用 getProperty 方法提供并行性。
        // 强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
        OutputStream fos = new FileOutputStream(filePath);
        props.setProperty(pKey, pValue);
        // 以适合使用 load 方法加载到 Properties 表中的格式，
        // 将此 Properties 表中的属性列表（键和元素对）写入输出流
        props.store(fos, "Update '" + pKey + "' value");

    }
}
