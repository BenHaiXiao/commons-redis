/*
 * Copyright (c) 2012 duowan.com. 
 * All Rights Reserved.
 * This program is the confidential and proprietary information of 
 * duowan. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with duowan.com.
 */
package com.github.benhaixiao.commons.redis.sharded;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * 
 * @author xiaobenhai
 */
public class ShardedJedisPoolManager{
	private static final Logger log = LoggerFactory.getLogger(ShardedJedisPoolManager.class);
	
	private List<Pool<ShardedJedis>> jedisPools;
	public ShardedJedis getJedis() {
		for (int i = 0; i < jedisPools.size(); i++) {
			Pool<ShardedJedis> jedisPool = jedisPools.get(i);
			try {
				ShardedJedis jedis = jedisPool.getResource();
				if(jedis != null){
					return jedis;
				}
			} catch (JedisConnectionException e) {
				log.warn("Get jedis from pool({}) failed", i, e);
			}
		}
		throw new JedisException("Cann't get jedis from all pool"); 
	}

	public void setJedisPools(List<Pool<ShardedJedis>> jedisPools) {
		this.jedisPools = jedisPools;
	}

	public List<Pool<ShardedJedis>> getJedisPools() {
		return jedisPools;
	}
	
	public void returnJedis(ShardedJedis jedis) {
		if(jedis != null) jedis.close();
	}
}
