package cn.pengzhaopeng.spark.utils;


import redis.clients.jedis.*;

import java.util.*;

/**
 * jedis 连接哨兵模式
 */
public class JedisSentinePoolUtil {

    public static void main(String[] args) {
        JedisSentinelPool pool = JedisSentinePoolUtil.getInstance();
        Jedis jedis = pool.getResource();
        // 执行两个命令
        jedis.set("k311", "给我一百块");
        String value = jedis.get("k311");
        System.out.println(value);

        //释放
        JedisSentinePoolUtil.close(jedis);
    }


    //被volatile修饰的变量不会被本地线程缓存，对该变量的读写都是直接操作共享内存。
    private static volatile JedisSentinelPool jedisSentinelPool = null;

    private JedisSentinePoolUtil() {
    }

    public static JedisSentinelPool getInstance() {
        if (null == jedisSentinelPool) {
            synchronized (JedisSentinePoolUtil.class) {
                if (null == jedisSentinelPool) {

                    //获取配置文件
                    String ips = PropertiesUtil.getString("redis.host");
//                    int port = PropertiesUtil.getInt("redis.port");
                    int sentinelsPort = PropertiesUtil.getInt("redis.sentinelsPort");
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
                    Set<String> sentinels = new HashSet<>();
                    if (StringUtils.isNoEmpty(ips)) {
                        String[] splitIp = ips.split(",");
                        for (String ip : splitIp) {
                            sentinels.add(ip + ":" + sentinelsPort);
                        }
                    }

                    if (StringUtils.isNoEmpty(password)) {
                        // 创建连接池
                        jedisSentinelPool = new JedisSentinelPool("mymaster", sentinels, jedisPoolConfig, password);
                    } else {
                        // 创建连接池
                        jedisSentinelPool = new JedisSentinelPool("mymaster", sentinels, jedisPoolConfig);
                    }

                }
            }
        }
        return jedisSentinelPool;
    }


    private static Jedis getJedis() {
        return jedisSentinelPool.getResource();
    }


    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }


}
