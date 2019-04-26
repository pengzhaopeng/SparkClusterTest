package cn.pengzhaopeng.spark.utils;

import java.io.UnsupportedEncodingException;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.text.DecimalFormat;

/**
 * Created by Administrator on 2017/12/15 0015.
 */

public class StringUtils {

    //格式化
    private static DecimalFormat dfs = null;

    //DecimalFormat用法   https://www.cnblogs.com/hq233/p/6539107.html
    public static DecimalFormat format(String pattern) {
        if (dfs == null) {
            dfs = new DecimalFormat();
        }
        dfs.setRoundingMode(RoundingMode.FLOOR);
        dfs.applyPattern(pattern);
        return dfs;
    }

    /**
     * 判断字符串是否为空 空返回false，反之返回true
     *
     * @param s
     * @return
     */
    public static boolean isNoEmpty(String s) {
        if (null == s || "".equals(s) || "null".equals(s) || "NULL".equals(s)
                || "[]".equals(s) || "<null>".equals(s) || "<NULL>".equals(s)) {
            return false;
        }
        return true;
    }

    /**
     * 请求参数包含中文时，设置UTF-8格式
     *
     * @param s
     * @return
     */
    public static String toURLEncoderUTF8(String s) {
        String result = "";
        try {
            result = URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 去掉特殊字符
     */
    public static String formatting(String str){
        if (!StringUtils.isNoEmpty(str)) {
            return "";
        }
        return str.replaceAll("[^0-9a-zA-Z\u4e00-\u9fa5.，,。？“”]+","");
    }


}
