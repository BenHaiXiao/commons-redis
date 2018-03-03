package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;

import com.github.benhaixiao.commons.redis.pool.ShardedJedisSentinelPool;

/**
 * @author xiaobenhai
 */
public class SardedJedisSentinelPoolTest{
  String DEV_IP = "172.27.136.4";
  String master1 = "selector_master_1";
  String master2 = "selector_master_2";
  String s1 = DEV_IP + ":36382";
  String s2 = DEV_IP + ":36392";
  
  private static final String MASTER_NAME = "mymaster";


  protected static HostAndPort sentinel1 = new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT);
  protected static HostAndPort sentinel2 = new HostAndPort("localhost", Protocol.DEFAULT_SENTINEL_PORT + 1);
  
  protected static Jedis sentinelJedis1;
  protected static Jedis sentinelJedis2;

  protected Set<String> sentinels = new HashSet<String>();
  protected List<String> masters = new ArrayList<String>();
  

  @Before
  public void setUp() throws Exception {
    sentinels.add(s1);
    sentinels.add(s2);
    masters.add(master1);
    masters.add(master2);
  }


  @Test
  public void checkCloseableConnections() throws Exception {
    final JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(300);
    config.setMinIdle(100);
    config.setMaxIdle(100);
    config.setMaxWaitMillis(5000L);
    config.setTestOnBorrow(false);
    config.setTestOnReturn(false);
//    config.setMinEvictableIdleTimeMillis(1000L);//ms
    config.setTimeBetweenEvictionRunsMillis(5000L);
    config.setNumTestsPerEvictionRun(-1);
    config.setTestWhileIdle(true);

//    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
//        "foobared", 2);
    ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masters, sentinels,config, 1000);
    ShardedJedis jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
    jedis.close();
    jedis = pool.getResource();
//    Jedis jedis = pool.getResource();
//    jedis.auth("foobared");
    jedis.set("foo", "bar");
    jedis.set("a","1");
    jedis.set("b","2");
//    System.out.println(jedis.get("foo"));
    assertEquals("bar", jedis.get("foo"));
    System.out.println(jedis.get("a"));
    System.out.println(jedis.get("b"));
    //pool.returnResource(jedis);
    jedis.close();
//    pool.close();
//    assertTrue(pool.isClosed());
    while(true){
    	 jedis = pool.getResource();
    	 jedis.close();
    	Thread.sleep(1000L);
    }
  }

}