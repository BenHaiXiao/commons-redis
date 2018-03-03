package com.github.benhaixiao.commons.redis;

import com.github.benhaixiao.commons.serializer.JavaSerializer;
import com.github.benhaixiao.commons.serializer.Serializater;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.*;
import redis.clients.util.SafeEncoder;

import java.io.Serializable;
import java.util.*;

/**
 * Jedis包装类
 * 
 * @author xiaobenhai
 */
public class JedisWrapper {
	protected JedisPoolManager jedisPoolManager;
	protected Serializater serializer = new JavaSerializer();

	public void setJedisPoolManager(JedisPoolManager jedisPoolManager) {
		this.jedisPoolManager = jedisPoolManager;
	}

	public void setSerializer(Serializater serializer) {
		this.serializer = serializer;
	}

	public Jedis getJedis() {
		return jedisPoolManager.getJedis();
	}

	public void returnJedis(Jedis jedis) {
		jedisPoolManager.returnJedis(jedis);
	}

	/**
	 * 某些需要重用jedis而不是每次都去池里取一个jedis实例情况下 可先获取jedis实例，再将实例作为参数可调用该方法 注：该方法不会关闭jedis，需要调用方自行关闭
	 * 
	 * @param jedis
	 * @param key
	 * @param obj
	 *            这里的对应应该实现Serializable接口，否则会报错，之所以参数类型不定义为Serializable的原因是因为有些常用集合类的接口是没实现Serializable接口的，比如List、Map等而它们的子类才有实现如ArrayList
	 * @param seconds
	 */
	public void set(Jedis jedis, String key, Object obj, int seconds) {
		if (StringUtils.isBlank(key) || obj == null)
			return;
		try {
			Serializable serial = (Serializable) obj;
			if (seconds > 0) {
				jedis.setex(SafeEncoder.encode(key), seconds, serializer.serialize(serial));
				//				FileUtils.writeByteArrayToFile(new File("/home/zhengdongjian/size/" + key), serialization.serialize(serial));
			} else {
				jedis.set(SafeEncoder.encode(key), serializer.serialize(serial));
				//				FileUtils.writeByteArrayToFile(new File("/home/zhengdongjian/size/" + key), serialization.serialize(serial));
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
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			set(jedis, key, obj, seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * set
	 * 
	 * @param jedis
	 * @param key
	 * @param obj
	 */
	public void set(Jedis jedis, String key, Object obj) {
		set(jedis, key, obj, -1);
	}

	/**
	 * @param key
	 * @param obj
	 * @throws JedisOperationException
	 */
	public void set(String key, Object obj) {
		set(key, obj, -1);
	}

	/**
	 * @param jedis
	 * @param key
	 * @param value
	 * @param seconds
	 * @throws JedisOperationException
	 */
	public void set(Jedis jedis, String key, String value, int seconds) {
		if (StringUtils.isBlank(key) || value == null)
			return;
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
	 * 
	 * @param jedis
	 * @param key
	 * @param value
	 */
	public void set(Jedis jedis, String key, String value) {
		set(jedis, key, value, -1);
	}

	/**
	 * 
	 * @param key
	 * @param value
	 * @param seconds
	 * @throws JedisOperationException
	 */
	public void set(String key, String value, int seconds) {
		if (StringUtils.isBlank(key) || value == null)
			return;
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			set(jedis, key, value, seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * set
	 * 
	 * @param key
	 * @param value
	 */
	public void set(String key, String value) {
		set(key, value, -1);
	}

	/**
	 * setnx
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public long setnx(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.setnx(key, value);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends Serializable> void mset(List<T> list, final int seconds, final KeyGenerator<T> keyGenerator) {
		this.multiSet(list, seconds, keyGenerator);
	}

	@SuppressWarnings("unchecked")
	public <T extends Serializable> void mset(List<T> list, final int seconds, final KeyGenerator<T> keyGenerator1,
			final KeyGenerator<T> keyGenerator2) {
		this.multiSet(list, seconds, keyGenerator1, keyGenerator2);
	}

	@SuppressWarnings("unchecked")
	public <T extends Serializable> void mset(List<T> list, final int seconds, final KeyGenerator<T> keyGenerator1,
			final KeyGenerator<T> keyGenerator2, final KeyGenerator<T>... keyGenerators) {
		this.multiSet(list, seconds, ArrayUtils.addAll(keyGenerators, keyGenerator1, keyGenerator2));
	}

	/**
	 * 批量set
	 * 
	 * @param list
	 * @param seconds
	 *            超时时间
	 * @param keyGenerators
	 */
	@SuppressWarnings("unchecked")
	private <T extends Serializable> void multiSet(List<T> list, final int seconds, final KeyGenerator<T>... keyGenerators) {
		if (seconds <= 0) {
			this.mset(list, keyGenerators);
		} else {
			this.pipeline(new MultiPipelineExecutor<T>() {
				@Override
				public void execute(Pipeline pipeline, T t) {
					for (KeyGenerator<T> kg : keyGenerators) {
						String key = kg.key(t);
						if (StringUtils.isNotBlank(key)) {
							pipeline.setex(SafeEncoder.encode(key), seconds, serializer.serialize(t));
						}
					}
				}
			}, list);
		}
	}

	/**
	 * 批量set
	 * 
	 * @param
	 * @return
	 * @throws JedisOperationException
	 */
	@SuppressWarnings("unchecked")
	public <T extends Serializable> void mset(List<T> list, final KeyGenerator<T>... keyGenerators) {
		Jedis jedis = null;
		try {
			byte[][] keysvalues = new byte[keyGenerators.length * list.size() * 2][];
			int i = 0;
			for (T t : list) {
				for (KeyGenerator<T> kg : keyGenerators) {
					keysvalues[i++] = SafeEncoder.encode(kg.key(t));
					keysvalues[i++] = serializer.serialize(t);
				}
			}
			jedis = jedisPoolManager.getJedis();
			jedis.mset(keysvalues);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * 批量set
	 * 
	 * @param
	 * @return
	 * @throws JedisOperationException
	 */
	public <T> void mset(List<T> list,final int seconds,final KeyGenerator<T> keyGenerator,final ValueGenerator<T> valueGenerator) {
		this.pipeline(new MultiPipelineExecutor<T>() {
			@Override
			public void execute(Pipeline pipeline, T t) {
					String key = keyGenerator.key(t);
					if (StringUtils.isNotBlank(key)) {
						pipeline.setex(SafeEncoder.encode(key), seconds, serializer.serialize(valueGenerator.value(t)));
					}
			}
		}, list);
	}

	/**
	 * 批量set
	 * 
	 * @param map
	 * @param seconds
	 *            超时时间
	 */
	public <V extends Serializable> void multiSet(final Map<String, V> map, final int seconds) {
		if (map == null || map.size() == 0)
			return;
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Pipeline pipeline = jedis.pipelined();
			for (String key : map.keySet()) {
				V param = map.get(key);
				byte[] serialize = serializer.serialize(param);
				pipeline.setex(SafeEncoder.encode(key), seconds, serialize);
			}
			pipeline.sync();
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
	public void delete(String... key) {
		Jedis jedis = null;
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
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.get(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 批量get
	 * 
	 * @param keys
	 * @return
	 * @throws JedisOperationException
	 */
	public List<String> mget(String... keys) {
		Jedis jedis = null;
		if (keys == null || keys.length == 0) {
			return new ArrayList<String>();
		}
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.mget(keys);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * get object
	 * 
	 * @param key
	 * @return 如果存在该KEY并且value不为空，则返回序列化后的对象，否则返回null
	 */
	public <T> T getObject(String key) {
		if (key == null)
			return null;
		Jedis jedis = null;
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

	@SuppressWarnings("unchecked")
	public <T> List<T> mgetObject(String... keys) {
		Jedis jedis = null;
		List<T> list = new ArrayList<T>();
		if (keys == null || keys.length == 0)
			return list;
		try {
			byte[][] byteKeys = new byte[keys.length][];
			for (int i = 0; i < keys.length; i++) {
				byteKeys[i] = SafeEncoder.encode(keys[i]);
			}
			jedis = jedisPoolManager.getJedis();
			List<byte[]> mget = jedis.mget(byteKeys);
			if (mget != null) {
				for (byte[] serialize : mget) {
					if (serialize != null && serialize.length > 0) {
						list.add((T) serializer.deserialize(serialize));
					}
				}
			}
			return list;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 匹配key
	 * 
	 * @param pattern
	 *            格式支持?*等通配
	 * @return
	 * @throws JedisOperationException
	 */
	public Set<String> getKeys(String pattern) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.keys(pattern);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 游标匹配key
	 * 
	 * @param pattern
	 *            格式支持?*等通配
	 * @return
	 * @throws JedisOperationException
	 */
	public List<String> scanKeys(String pattern) {
		Jedis jedis = null;
		try {
			String cursor = ScanParams.SCAN_POINTER_START;
			ScanParams scanParams = new ScanParams();
			scanParams.match(pattern);
			boolean cycleIsFinished = false;
			List<String> ret = new ArrayList<String>();
			jedis = jedisPoolManager.getJedis();
			while (!cycleIsFinished) {
				ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
				List<String> data = scanResult.getResult();
				cursor = scanResult.getStringCursor();
				ret.addAll(data);
				if ("0".equals(cursor)) {
					cycleIsFinished = true;
				}
			}
			return ret;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 匹配key的数量
	 * 
	 * @param pattern
	 *            格式支持?*等通配
	 * @return
	 * @throws JedisOperationException
	 */
	public int getKeySize(String pattern) {
		Set<String> keys = getKeys(pattern);
		return keys.size();
	}

	/**
	 * 检查给定 key 是否存在
	 * 
	 * @param key
	 * @return 若 key 存在，返回 1 ，否则返回 0
	 */
	public boolean exists(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.exists(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * incr
	 * 
	 * @param key
	 * @return
	 */
	public Long incr(String key) {
		Jedis jedis = null;
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
	 * 
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long incrBy(String key, long integer) {
		Jedis jedis = null;
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
	 * 
	 * @param key
	 * @return
	 */
	public Long decr(String key) {
		Jedis jedis = null;
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
	 * 
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long decrBy(String key, long integer) {
		Jedis jedis = null;
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
	 * subscribe
	 * 
	 * @param jedisPubSub
	 * @param channels
	 */
	public void subscribe(JedisPubSub jedisPubSub, String[] channels) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.subscribe(jedisPubSub, channels);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * publish
	 * 
	 * @param channel
	 * @param message
	 * @return
	 */
	public long publish(String channel, String message) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.publish(channel, message);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * pipeline
	 * 
	 * @param pipelineExecutor
	 * @param list
	 */
	public <T> void pipeline(MultiPipelineExecutor<T> pipelineExecutor, Collection<T> list) {
		if (list == null || list.size() == 0)
			return;

		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Pipeline pipeline = jedis.pipelined();
			for (T obj : list) {
				pipelineExecutor.execute(pipeline, obj);
			}
			pipeline.sync();
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * pipeline
	 * 
	 * @param pipelineExecutor
	 */
	public void pipeline(PipelineExecutor pipelineExecutor) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Pipeline pipeline = jedis.pipelined();
			pipelineExecutor.execute(pipeline);
			pipeline.sync();
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
	 * @param seconds
	 *            过期时间(秒)
	 * @return 成功是返回1，否则返回0
	 * @throws JedisOperationException
	 */
	public Long expire(String key, int seconds) {
		Jedis jedis = null;
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
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.expireAt(key, unixTime);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * @param key
	 * @param score
	 * @param member
	 * @return 新增元素数量
	 */
	public long zadd(String key, double score, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long addMemberCount = jedis.zadd(key, score, member);
			return addMemberCount;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * @param key
	 * @param members
	 * @return 新增元素数量
	 */
	public long zadd(String key, Map<String, Double> members) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long addMemberCount = jedis.zadd(key, members);
			return addMemberCount;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * @param key
	 * @param score
	 * @param member
	 * @return 修改后的元素的分值
	 */
	public Double zincrby(String key, double score, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Double newScore = jedis.zincrby(key, score, member);
			return newScore;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 按照分数从低到高,返回一定排名范围的元素和对应分值
	 *
	 * @param key
	 * @param start
	 *            开始的排名,从0开始
	 * @param end
	 *            不包含的最后的排名.开始结束排名传0和-1 得到整个有序集合所有元素
	 * @return
	 */
	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Set<Tuple> tuples = jedis.zrangeWithScores(key, start, end);
			return tuples;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * 取score的min至max元素，带score值
	 */
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Set<Tuple> tuples = jedis.zrangeByScoreWithScores(key, min, max);
			return tuples;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 按照分数从高到低,返回一定排名范围的元素
	 *
	 * @param key
	 * @param start
	 *            开始的排名,从0开始
	 * @param end
	 *            不包含的最后的排名.开始结束排名传0和-1 得到整个有序集合所有元素
	 * @return
	 */
	public Set<String> zrevrange(String key, long start, long end) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Set<String> members = jedis.zrevrange(key, start, end);
			return members;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	/**
	 * 按照分数从高到低,返回一定排名范围的元素和对应分值
	 *
	 * @param key
	 * @param start
	 *            开始的排名,从0开始
	 * @param end
	 *            不包含的最后的排名.开始结束排名传0和-1 得到整个有序集合所有元素
	 * @return
	 */
	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Set<Tuple> tuples = jedis.zrevrangeWithScores(key, start, end);
			return tuples;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 查询一个有序集合中某个元素的排名,按照分数从低到高排序.第一名是0
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public Long zrevrank(String key, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long zrevrank = jedis.zrevrank(key, member);
			return zrevrank;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 查询一个有序集合中某个元素的排名,按照分数从高到低排序.第一名是0
	 *
	 * @param key
	 * @param member
	 * @return
	 */
	public Long zrank(String key, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long zrank = jedis.zrank(key, member);
			return zrank;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	public Double zscore(String key, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Double zscore = jedis.zscore(key, member);
			return zscore;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	public long zrem(String key, String... members) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long removeCount = jedis.zrem(key, members);
			return removeCount;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

    public Long zcard(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolManager.getJedis();
            Long size = jedis.zcard(key);
            return size;
        } catch (Exception e) {
            throw new JedisOperationException(e);
        } finally {
            jedisPoolManager.returnJedis(jedis);
        }
    }
    
    public String hmset(String key, Map<String, String> hash) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		String ret = jedis.hmset(key, hash);
    		return ret;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    public List<String> hmget(String key, String... fields) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		List<String> ret = jedis.hmget(key, fields);
    		return ret;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    public Long hset(String key, String field, String value) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		Long ret = jedis.hset(key, field, value);
    		return ret;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
	public Long hsetnx(String key, String field, String value){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long ret = jedis.hsetnx(key, field, value);
			return ret;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

    public <T extends Serializable> Long hsetObject(String key, String field, T value) {
    	Jedis jedis = null;
    	try {
    		Serializable serial = (Serializable)value;
    		byte[] serialize = serializer.serialize(serial);
    		jedis = jedisPoolManager.getJedis();
    		Long ret = jedis.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), serialize);
    		return ret;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    public String hget(String key, String field) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		String value = jedis.hget(key, field);
    		return value;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    public ScanResult<Map.Entry<String, String>> hscan(String key,String cursor,ScanParams params){
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		ScanResult<Map.Entry<String, String>> value = jedis.hscan(key, cursor , params);
    		return value;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    
    public <T extends Serializable> T hgetObjcet(String key, String field) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		byte[] value = jedis.hget(SafeEncoder.encode(key), SafeEncoder.encode(field));
			if (value != null && value.length != 0) {
				T deserialize = serializer.deserialize(value);
				return deserialize;
			}
			return null;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
	}

    public Long hdel(String key, String... field) {
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		Long value = jedis.hdel(key, field);
    		return value;
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }

    public Long hlen(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			Long len = jedis.hlen(key);
			return len;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

    public Set<String> hkeys(String key){
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		return jedis.hkeys(key);
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    
    public Boolean hexists(String key, String field){
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		return jedis.hexists(key, field);
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    
    public List<String> hvals(String key){
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		return jedis.hvals(key);
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    
    public <T extends Serializable> String hmsetObject(String key, Map<String, T> hash) {
    	Map<byte[], byte[]> values = new HashMap<byte[], byte[]>(hash.size());
    	for(String k : hash.keySet()) {
    		Serializable serial = (Serializable)hash.get(k);
    		values.put(SafeEncoder.encode(k), serializer.serialize(serial));
    	}
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		return jedis.hmset(SafeEncoder.encode(key), values);
    	} catch (Exception e) {
    		throw new JedisOperationException(e);
    	} finally {
    		jedisPoolManager.returnJedis(jedis);
    	}
    }
    
    public <T> List<T> hmgetObject(String key, String... fields) {
    	List<T> ret = new ArrayList<T>(fields.length);
    	byte[][] fieldsBt = new byte[fields.length][];
    	for(int i=0; i<fields.length;i++) {
    		fieldsBt[i] = SafeEncoder.encode(fields[i]); 
    	}
    	Jedis jedis = null;
    	try {
    		jedis = jedisPoolManager.getJedis();
    		List<byte[]> vals = jedis.hmget(SafeEncoder.encode(key), fieldsBt);
    		for(int i=0; i<fields.length;i++) {
    			T deserialize = serializer.deserialize(vals.get(i));
    			if(deserialize != null)
    				ret.add(deserialize);
    		}
			return ret;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}


	public Map<String, String> hgetall(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.hgetAll(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 
	 * @param key
	 * @param seconds
	 * @param members
	 * @throws JedisOperationException
	 */
	public void sadd(String key, int seconds, String... members) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.sadd(key, members);
			jedis.expire(key, seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 *
	 * @param key
	 * @param members
	 * @throws JedisOperationException
	 */
	public void sadd(String key, String... members) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.sadd(key, members);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	public Set<String> smembers(String key) {
		Set<String> values = Sets.newHashSet();
		if (key == null) return null;
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			values = jedis.smembers(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
		return values;
	}
	
	public boolean sismember(String key,String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.sismember(key, member);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	public void srem(String key,String... members){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.srem(key,members);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

    public String spop(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolManager.getJedis();
            return jedis.spop(key);
        } catch (Exception e) {
            throw new JedisOperationException(e);
        }finally {
            jedisPoolManager.returnJedis(jedis);
        }
    }

    public Set<String> sdiff(String... keys) {
        Jedis jedis = null;
        try {
            jedis = jedisPoolManager.getJedis();
            return jedis.sdiff(keys);
        } catch (Exception e) {
            throw new JedisOperationException(e);
        }finally {
            jedisPoolManager.returnJedis(jedis);
        }
    }

	public void lpushTrim(String key, String value, int size) {
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.lpush(key, value);
			if (size > 0)
				jedis.ltrim(key, 0, size - 1);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	public <T extends Serializable> void lpushTrimObject(String key, T value, int size) {
		Jedis jedis = null;
		try {
			Serializable serial = (Serializable)value;
    		byte[] serialize = serializer.serialize(serial);
    		jedis = jedisPoolManager.getJedis();
    		jedis.lpush(SafeEncoder.encode(key), serialize);
			if (size > 0)
				jedis.ltrim(key, 0, size - 1);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	public List<String> lrange(String key,long start,long stop){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.lrange(key, start, stop);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	public <T extends Serializable> List<T> lrangeObject(String key,long start,long stop){
		Jedis jedis = null;
		List<T> list = new ArrayList<T>();
		try {
			jedis = jedisPoolManager.getJedis();
    		List<byte[]> value = jedis.lrange(SafeEncoder.encode(key), start, stop);
			for(byte[] val : value) {
				T deserialize = serializer.deserialize(val);
				list.add(deserialize);
			}
			return list;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	//zadd 有序集合的存取
	 public void zadd(String key,int score,String member,int seconds){
	    Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.zadd(key,score,member);
			jedis.expire(key, seconds);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	 }
		 
	 //取出有序的值,从小到大
	 public Set<Tuple> zrangMemberWithScore(String key,int start,int end){
		Set<Tuple> values = Sets.newHashSet();
		if (key == null) return null;
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			values = jedis.zrangeWithScores(key, start, end);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
		return values;
	 }
	 
	 //取出有序的值,从小到大
	 public Set<String> zrangMember(String key,int start,int end){
		Set<String> values = Sets.newHashSet();
		if (key == null) return null;
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			values = jedis.zrange(key, start, end);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
		return values;
	 }
	 //删除某个key
	 public void deleteMember(String key,int start,int end){
	    Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			jedis.zremrangeByRank(key, start, end);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	 }
	 
	 public static interface PipelinedCallback{
		 /**
		  * pipeline 回掉方法
		  * 
		 * @param pipeline 启动了的pipeline
		 * @param serializer 序列化工具，pipeline不支持setObject，这里请自己序列化对象再插入
		 */
		void execute(Pipeline pipeline,Serializater serializer);
	 }
	 
	 /**
	  * 同步批量执行
	  * 
	 * @param callback 操作
	 * @return 返回结果
	 */
	public List<Object> pipelined(PipelinedCallback callback){
		 Jedis jedis = null;
			try {
				jedis = jedisPoolManager.getJedis();
				Pipeline pipeline = jedis.pipelined();
				
				callback.execute(pipeline,serializer);
				
				return pipeline.syncAndReturnAll();
			} catch (Exception e) {
				throw new JedisOperationException(e);
			} finally {
				jedisPoolManager.returnJedis(jedis);
			}
	 }


	/**
     * eval
	 *
	 * @param script lua脚本
	 * @return 返回结果
	 */
	public Object eval(String script){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.eval(script);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 * 返回集合个数
	 * @param key
	 * @return
	 */
	public Long scard(String key){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.scard(key);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
	
	/**
	 * 返回交集内容
	 * @param key
	 * @return
	 */
	public Set<String> sunion(String... keys){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.sunion(keys);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public String lpop(String key){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.lpop(key) ;
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public Long lpush(String key,String... strings){
		Jedis jedis = null;
		try {
			jedis = jedisPoolManager.getJedis();
			return jedis.lpush(key, strings);
		} catch (Exception e) {
			throw new JedisOperationException(e);
		} finally {
			jedisPoolManager.returnJedis(jedis);
		}
	}
}
