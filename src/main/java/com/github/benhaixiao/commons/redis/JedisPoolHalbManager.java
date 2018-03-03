/*
 * Copyright (c) 2012 duowan.com. 
 * All Rights Reserved.
 * This program is the confidential and proprietary information of 
 * duowan. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with duowan.com.
 */
package com.github.benhaixiao.commons.redis;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * @author xiaobenhai
 */
public class JedisPoolHalbManager extends AbstractJedisPoolManager{
	private static final Logger log = LoggerFactory.getLogger(JedisPoolHalbManager.class);
	
	private List<Pool<Jedis>> jedisPools;
	public Jedis getJedis() {
		for (int i = 0; i < jedisPools.size(); i++) {
			Pool<Jedis> jedisPool = jedisPools.get(i);
			try {
				Jedis jedis = jedisPool.getResource();
				if(jedis != null){
					return jedis;
				}
			} catch (JedisConnectionException e) {
				log.warn("Get jedis from pool({}) failed", i, e);
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
}
