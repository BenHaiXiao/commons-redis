package com.github.benhaixiao.commons.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * 轮询机制
 * @author xiaobenhai
 */
public class JedisPoolHalbRRManager extends AbstractJedisPoolManager {
	private static final Logger log = LoggerFactory.getLogger(JedisPoolHalbRRManager.class);

	private AtomicInteger seq = new AtomicInteger(0);

	private List<Pool<Jedis>> jedisPools;
	private int SIZE = 0;
	/**默认超时时间3秒*/
	public static final int DEFAULT_TIMEOUT = 3000;
	private int timeout = DEFAULT_TIMEOUT;
	private static final String SPLIT1 = ",", SPLIT2 = ":";
	protected int database = 0;
	
	public JedisPoolHalbRRManager(){}
	
	/**
	 * @param servers 以ip:port中间用逗号隔开
	 * @param poolConfig 池配置
	 * @param timeout 超时时间
	 */
	public JedisPoolHalbRRManager(GenericObjectPoolConfig poolConfig,String servers,int timeout){
		this(poolConfig,servers,timeout,0);
	}
	
	public JedisPoolHalbRRManager(GenericObjectPoolConfig poolConfig,String servers,int timeout,int database){
		jedisPools = new ArrayList<Pool<Jedis>>();
		setTimeout(timeout);
		String[] serverArray = servers.split(SPLIT1);
		for(String serverPort : serverArray){
			String[] ipPort = serverPort.split(SPLIT2);
			JedisPool pool = new JedisPool(poolConfig, ipPort[0], Integer.parseInt(ipPort[1]), timeout,null,database);
			jedisPools.add(pool);
		}
		SIZE = jedisPools.size();
		if(SIZE == 0) throw new RuntimeException("Jedis Pool Size is 0");
	}
	
	/**
	 * 默认超时时间为3秒
	 * @param servers 以ip:port中间用逗号隔开
	 * @param poolConfig 池配置
	 */
	public JedisPoolHalbRRManager(GenericObjectPoolConfig poolConfig,String servers){
		this(poolConfig,servers,DEFAULT_TIMEOUT);
		
	}
	
	public Jedis getJedis() {
		int next = Math.abs(seq.getAndIncrement());
		for (int j = 0; j < SIZE; j++) {
			int p = (next + j) % SIZE;
			Pool<Jedis> jedisPool = jedisPools.get(p);
			try {
				Jedis jedis = jedisPool.getResource();
				if (jedis != null) {
					return jedis;
				}
			} catch (JedisConnectionException e) {
				log.warn("Get jedis from pool({}) failed", p, e);
			}
		}
		throw new JedisException("Cann't get jedis from all pool");
	}

	public void setJedisPools(List<Pool<Jedis>> jedisPools) {
		this.jedisPools = jedisPools;
	}

	public List<Pool<Jedis>> getJedisPools() {
		return jedisPools;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}
}
