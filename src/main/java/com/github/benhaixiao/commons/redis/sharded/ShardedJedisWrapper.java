package com.github.benhaixiao.commons.redis.sharded;

import java.io.Serializable;

import com.github.benhaixiao.commons.serializer.JavaSerializer;
import com.github.benhaixiao.commons.serializer.Serializater;
import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.ShardedJedis;
import redis.clients.util.SafeEncoder;

import com.github.benhaixiao.commons.redis.JedisOperationException;

/**
 * Jedis包装类
 * @author xiaobenhai
 */
public class ShardedJedisWrapper {
	
	protected ShardedJedisPoolManager jedisPoolManager;
	protected Serializater serializer = new JavaSerializer();

	public void setJedisPoolManager(ShardedJedisPoolManager jedisPoolManager) {
		this.jedisPoolManager = jedisPoolManager;
	}
	
	public void setSerializer(Serializater serializer) {
		this.serializer = serializer;
	}

	public Serializater getSerializer() {
		return serializer;
	}

	public ShardedJedis getJedis(){
		return jedisPoolManager.getJedis();
	}
	 
	public void returnJedis(ShardedJedis jedis){
		jedisPoolManager.returnJedis(jedis);
	}

	/**
	 * 某些需要重用jedis而不是每次都去池里取一个jedis实例情况下
	 * 可先获取jedis实例，再将实例作为参数可调用该方法
	 * 注：该方法不会关闭jedis，需要调用方自行关闭
	 * @param jedis
	 * @param key
	 * @param obj 这里的对应应该实现Serializable接口，否则会报错，之所以参数类型不定义为Serializable的原因是因为有些常用集合类的接口是没实现Serializable接口的，比如List、Map等而它们的子类才有实现如ArrayList
	 * @param seconds
	 */
	public void set(ShardedJedis jedis,String key, Object obj, int seconds) {
		if (StringUtils.isBlank(key) || obj == null) return;
		try {
			byte[] value = null;
			if(obj  instanceof byte[]){
				value = (byte[])obj;
			}else {
				Serializable serial = (Serializable)obj;
				value = serializer.serialize(serial);
			}
			if (seconds > 0) {
				jedis.setex(SafeEncoder.encode(key), seconds,value);
			} else {
				jedis.set(SafeEncoder.encode(key), value);
			}
		} catch (Exception e) {
			throw new JedisOperationException(e);
		}
	}
	
	/**
	 * 
	 * @param key
	 * @param obj
	 * @param seconds
	 * @throws JedisOperationException
	 */
	public void set(String key, Object obj, int seconds) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			set(jedis,key,obj,seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * set
	 * @param jedis
	 * @param key
	 * @param obj
	 */
	public void set(ShardedJedis jedis,String key, Object obj) {
		set(jedis,key,obj,-1);
	}
	
	/**
	 * @param key
	 * @param c
	 * @throws JedisOperationException
	 */
	public void set(String key, Object obj) {
		set(key,obj,-1);
	}
	
	/**
	 * @param jedis
	 * @param key
	 * @param value
	 * @param seconds
	 * @throws JedisOperationException
	 */
	public void set(ShardedJedis jedis,String key, String value, int seconds) {
		if (StringUtils.isBlank(key) || value == null) return;
		try {
			if (seconds > 0) {
				jedis.setex(key, seconds, value);
			} else {
				jedis.set(key, value);
			}
		} catch (Exception e) {
			throw new JedisOperationException(e);
		}
	}
	
	/**
	 * set
	 * @param jedis
	 * @param key
	 * @param value
	 */
	public void set(ShardedJedis jedis,String key, String value) {
		set(jedis,key, value, -1);
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 * @param seconds
	 * @throws JedisOperationException
	 */
	public void set(String key, String value, int seconds) {
		if (StringUtils.isBlank(key) || value == null) return;
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			set(jedis,key,value,seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * set
	 * @param key
	 * @param value
	 */
	public void set(String key, String value) {
		set(key, value, -1);
	}
	
	/**
	 * setnx
	 * @param key
	 * @param string
	 * @return
	 */
	public long setnx(String key, String value) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.setnx(key, value);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * 删除给定的一个 key
	 * 
	 * @param key
	 * @throws JedisOperationException
	 */
	public void delete(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.del(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 
	 * @param key
	 * @return 对应key的值
	 * @throws JedisOperationException
	 */
	public String get(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.get(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		}finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	public String getSet(String key, String value) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.getSet(key, value);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * get object
	 * @param key
	 * @return
	 */
	public <T> T getObject(String key){
		if (key == null) return null;
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			byte[] sz = jedis.get(SafeEncoder.encode(key));
			if (sz != null && sz.length > 0) {
				return serializer.deserialize(sz);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	

	/**
	 * 检查给定 key 是否存在
	 * 
	 * @param key
	 * @return 若 key 存在，返回 1 ，否则返回 0
	 */
	public boolean exists(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.exists(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		}finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * incr
	 * @param key
	 * @return
	 */
	public Long incr(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.incr(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * incr by
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long incrBy(String key, long integer) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.incrBy(key, integer);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * decr
	 * @param key
	 * @return
	 */
	public Long decr(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.decr(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * decr by
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long decrBy(String key, long integer) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
		    return jedis.decrBy(key, integer);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 设置过期
	 * 
	 * @param key 
	 * @param seconds 过期时间(秒)
	 * @return 成功是返回1，否则返回0
	 * @throws JedisOperationException 
	 */
	public Long expire(String key, int seconds) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.expire(key, seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	public long expireAt(String key, long unixTime) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.expireAt(key, unixTime);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	
	public long append(String key, String value) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.append(key, value);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	public long append(String key, String value,int seconds) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			long l = jedis.append(key, value);
			jedis.expire(key, seconds);
			return l;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	public long ttl(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			long l = jedis.ttl(key);
			return l;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
}
