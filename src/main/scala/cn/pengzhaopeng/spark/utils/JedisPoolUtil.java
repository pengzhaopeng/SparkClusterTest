package cn.pengzhaopeng.spark.utils;


import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

/**
 * jedis 连接哨兵模式
 */
public class JedisPoolUtil {

    public static void main(String[] args) {
        JedisCluster jedisCluster = JedisPoolUtil.getInstance();
        jedisCluster.set("k122", "狗子你变了");
        String value = jedisCluster.get("k12");
        System.out.println(value);

        //释放
        JedisPoolUtil.close(jedisCluster);
    }


    //被volatile修饰的变量不会被本地线程缓存，对该变量的读写都是直接操作共享内存。
    private static volatile JedisCluster jedisCluster = null;

    private JedisPoolUtil() {
    }

    public static JedisCluster getInstance() {
        if (null == jedisCluster) {
            synchronized (JedisPoolUtil.class) {
                if (null == jedisCluster) {

                    //获取配置文件
                    String ips = PropertiesUtil.getString("redis.host");
                    int port = PropertiesUtil.getInt("redis.port");
//                    int sentinelsPort = PropertiesUtil.getInt("redis.sentinelsPort");
                    String password = PropertiesUtil.getString("redis.password");

                    //设置相关配置
                    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                    jedisPoolConfig.setMaxTotal(PropertiesUtil.getInt("redis.maxTotal"));
                    jedisPoolConfig.setMaxIdle(PropertiesUtil.getInt("redis.maxIdle"));
                    jedisPoolConfig.setMaxWaitMillis(PropertiesUtil.getLong("redis.maxWaitMillis"));
                    jedisPoolConfig.setTestOnBorrow(PropertiesUtil.getBoolean("redis.testOnBorrow"));

                    // 哨兵信息
//                    Set<String> sentinels = new HashSet<String>(Arrays.asList("192.168.2.4:26379",
//                            "192.168.2.5:26379", "192.168.2.6:26379"));
                    Set<HostAndPort> nodes = new HashSet<>();
                    if (StringUtils.isNoEmpty(ips)) {
                        String[] splitIp = ips.split(",");
                        for (String ip : splitIp) {
                            nodes.add(new HostAndPort(ip,port));
                        }
                    }

                    // 创建连接池 这里没加密码判断
                    jedisCluster = new JedisCluster(nodes);

                }
            }
        }
        return jedisCluster;
    }


//    private static Jedis getJedis() {
//        return jedisSentinelPool.getResource();
//    }


    public static void close(JedisCluster jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }


}
